import {
	ChangeStream,
	Collection,
	Document,
	MongoClient,
	ResumeToken,
} from 'mongodb'
import { OffsetStorage } from '../storage/offset-storage'
import {
	ChangeStreamEvent,
	ChangeStreamWatchOptions,
	ConsumerConfig,
	ConsumerRecord,
	MessageHandlerConfig,
} from '../types'
import { BackoffManager } from '../utils/backoff'
import { Logger } from '../utils/logger'
import { ConsumerGroupManager } from './consumer-group'

export class ChangeStreamConsumer {
	private client: MongoClient | null = null
	private changeStream: ChangeStream | null = null
	private isRunning = false
	private isPaused = false

	private commitTimer: NodeJS.Timeout | null = null
	private consumerId: string
	private retryCount = 0
	private currentHandlerConfig: MessageHandlerConfig<Document> | null = null

	private hasUncommittedChanges = false
	private lastProcessedOffset: ResumeToken | null = null

	private changeStreams: Map<number, ChangeStream> = new Map()
	private partitionOffsets: Map<number, ResumeToken> = new Map()
	private partitionCommittedOffsets: Map<number, ResumeToken> = new Map()
	private partitionUncommittedChanges: Map<number, boolean> = new Map()

	private partitionBackoffs: Map<number, BackoffManager> = new Map()
	private partitionReconnecting: Map<number, boolean> = new Map()

	private logger: typeof Logger

	constructor(
		private config: ConsumerConfig,
		private offsetStorage: OffsetStorage,
		private consumerGroupManager: ConsumerGroupManager,
		private mongoUri: string,
		private database: string,
	) {
		this.consumerId = this.generateConsumerId()
		this.logger = Logger.withContext(`Consumer - ${this.consumerId}`)
	}

	private generateConsumerId(): string {
		return `${this.config.groupId}-${this.config.topic}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`
	}

	// Implementação da interface ChangeStreamConsumer
	getConsumerId(): string {
		return this.consumerId
	}
	getGroupId(): string {
		return this.config.groupId
	}
	getTopic(): string {
		return this.config.topic
	}
	getLastProcessedOffset(): ResumeToken | null {
		return this.lastProcessedOffset
	}
	isConnected(): boolean {
		return this.isRunning
	}
	isSubscribed(): boolean {
		return this.changeStream !== null
	}

	// Métodos para monitoramento e debug
	getOffsetInfo() {
		// Coletar informações de todas as partições
		const partitionOffsets = Array.from(this.partitionOffsets.entries()).map(
			([partition, offset]) => ({
				partition,
				processedOffset: offset,
				committedOffset: this.partitionCommittedOffsets.get(partition),
				hasUncommitted:
					this.partitionUncommittedChanges.get(partition) || false,
			}),
		)

		// Encontrar a última offset processada entre todas as partições
		const lastProcessedOffset = this.findLatestOffset(
			Array.from(this.partitionOffsets.values()),
		)
		const lastCommittedOffset = this.findLatestOffset(
			Array.from(this.partitionCommittedOffsets.values()),
		)

		// Verificar se há alguma partição com mudanças não commitadas
		const hasUncommittedChanges = Array.from(
			this.partitionUncommittedChanges.values(),
		).some((hasChanges) => hasChanges)

		return {
			lastProcessed: lastProcessedOffset,
			lastCommitted: lastCommittedOffset,
			hasUncommitted: hasUncommittedChanges,
			areOffsetsSynced:
				lastProcessedOffset && lastCommittedOffset
					? this.isSameOffset(lastProcessedOffset, lastCommittedOffset)
					: lastProcessedOffset === lastCommittedOffset, // ambos null = synced
			partitions: partitionOffsets,
			totalPartitions: this.config.partitions.length,
			activePartitions: this.partitionOffsets.size,
		}
	}

	// Método auxiliar para encontrar a offset mais recente
	private findLatestOffset(offsets: ResumeToken[]): ResumeToken | null {
		const validOffsets = offsets.filter(
			(offset) => offset !== null && offset !== undefined,
		)
		if (validOffsets.length === 0) return null

		// Como não temos timestamp nas offsets, retornamos a primeira não-nula
		// Em um sistema real, você precisaria de lógica mais sofisticada aqui
		return validOffsets[0]
	}

	// Log estado dos offsets periodicamente (opcional)
	startOffsetMonitoring() {
		if (this.config.enableOffsetMonitoring) {
			setInterval(() => {
				const offsetInfo = this.getOffsetInfo()

				this.logger.info('📊 OFFSET MONITORING:', {
					consumerId: this.consumerId,
					topic: this.config.topic,
					groupId: this.config.groupId,
					summary: {
						lastProcessed: offsetInfo.lastProcessed ? 'SET' : 'NULL',
						lastCommitted: offsetInfo.lastCommitted ? 'SET' : 'NULL',
						hasUncommitted: offsetInfo.hasUncommitted,
						areOffsetsSynced: offsetInfo.areOffsetsSynced,
					},
					partitions: offsetInfo.partitions.map((p) => ({
						partition: p.partition,
						status: p.processedOffset
							? p.hasUncommitted
								? 'UNCOMMITTED'
								: 'COMMITTED'
							: 'NO_OFFSET',
						processed: p.processedOffset ? 'YES' : 'NO',
						committed: p.committedOffset ? 'YES' : 'NO',
					})),
					totals: {
						totalPartitions: offsetInfo.totalPartitions,
						activePartitions: offsetInfo.activePartitions,
						partitionsWithOffsets: offsetInfo.partitions.filter(
							(p) => p.processedOffset,
						).length,
					},
				})
			}, this.config.autoCommitIntervalMs || 30000) // Usar o mesmo intervalo do auto-commit
		}
	}

	async connect(): Promise<void> {
		this.client = new MongoClient(this.mongoUri)
		await this.client.connect()

		let group = this.consumerGroupManager.getConsumerGroup(this.config.groupId)
		if (!group) {
			group = this.consumerGroupManager.createConsumerGroup(
				this.config.groupId,
				[this.config.topic],
			)
		}

		this.consumerGroupManager.addMemberToGroup(
			this.config.groupId,
			this.consumerId,
			this,
		)
		this.isRunning = true

		this.logger.info(
			`Consumer ${this.consumerId} connected to MongoDB and joined group ${this.config.groupId}`,
		)
	}

	async subscribe<T extends Document = Document>(
		config: MessageHandlerConfig<T>,
	): Promise<void> {
		if (!this.isRunning) {
			throw new Error('Consumer not connected. Call connect() first.')
		}

		if (!config) {
			throw new Error('MessageHandlerConfig is required to subscribe.')
		}

		// Armazenar a configuração atual
		this.currentHandlerConfig = config as MessageHandlerConfig<Document>

		// Inicializar estruturas para cada partição
		for (const partition of this.config.partitions) {
			this.partitionOffsets.set(partition, null)
			this.partitionCommittedOffsets.set(partition, null)
			this.partitionUncommittedChanges.set(partition, false)
			this.partitionBackoffs.set(
				partition,
				new BackoffManager(
					this.config.maxRetries || 10,
					this.config.retryDelayMs || 1000,
					30000, // maxDelay
				),
			)
			this.partitionReconnecting.set(partition, false)
		}

		// Criar change stream para cada partição
		for (const partition of this.config.partitions) {
			await this.createPartitionStream(partition)
		}

		// this.setupEventHandlers()
		this.setupAutoCommit()
		this.startOffsetMonitoring()

		this.logger.info(
			`Consumer ${this.consumerId} subscribed to partitions: ${this.config.partitions.join(', ')} of topic ${this.config.topic}`,
		)
	}

	private getPartitionCollectionName(partition: number): string {
		return `${this.config.topic}_p${partition}`
	}

	private async createPartitionStream(partition: number): Promise<void> {
		if (!this.client) {
			throw new Error('MongoClient is not initialized')
		}

		const collectionName = this.getPartitionCollectionName(partition)
		const db = this.client.db(this.database)

		// VERIFICAR SE A COLLECTION EXISTE
		const collections = await db
			.listCollections({ name: collectionName })
			.toArray()
		if (collections.length === 0) {
			this.logger.error(`Collection ${collectionName} does not exist!`, {
				partition,
				topic: this.config.topic,
			})
			throw new Error(`Collection ${collectionName} not found`)
		}

		const collection = db.collection(collectionName)

		// Buscar último offset commitado para esta partição
		const lastOffset = await this.offsetStorage.getOffset(
			this.config.groupId,
			this.config.topic,
			partition,
		)

		const options: ChangeStreamWatchOptions = {
			fullDocument: 'updateLookup' as const,
			batchSize: this.config.options?.batchSize || 100,
			maxAwaitTimeMS: this.config.options?.maxAwaitTimeMS || 1000,
		}

		if (lastOffset) {
			options.resumeAfter = lastOffset
			this.logger.info('Resuming from stored offset', {
				partition,
				hasResumeToken: true,
				fromBeginning: false,
			})
		} else if (this.config.fromBeginning) {
			try {
				// Tentar encontrar a primeira operação disponível
				const firstDoc = await collection
					.find()
					.sort({ _id: 1 })
					.limit(1)
					.toArray()
				if (firstDoc.length > 0) {
					// Se há documentos, iremos processá-los antes de iniciar o change stream
					await this.processExistingDocuments(collection, partition)

					this.logger.info('Starting from beginning (first document)', {
						partition,
						firstDocumentId: firstDoc[0]?._id,
					})
				} else {
					// Collection vazia - começar do momento atual
					this.logger.info('Starting from current (empty collection)', {
						partition,
					})
				}

				this.logger.info(
					'Starting from beginning (no stored offset + fromBeginning=true)',
					{
						partition,
						hasResumeToken: false,
						fromBeginning: true,
					},
				)
			} catch (error) {
				if (error instanceof Error)
					this.logger.warn(
						'Could not determine first document, starting from current',
						{
							partition,
							error: error.message,
						},
					)
			}
		} else {
			// Se não há offset armazenado E fromBeginning é false, começar do momento atual
			// O MongoDB Change Stream por padrão começa do momento atual quando não há resumeAfter
			this.logger.info(
				'Starting from current (no stored offset + fromBeginning=false)',
				{
					partition,
					hasResumeToken: false,
					fromBeginning: false,
				},
			)
		}

		try {
			const changeStream = collection.watch([], options)

			// ADICIONAR LOGS PARA DEBUG
			changeStream.on('change', async (change: ChangeStreamEvent<Document>) => {
				this.logger.info('Change received', {
					partition,
					operationType: change.operationType,
				})

				if (this.isPaused || !change.fullDocument) {
					this.logger.info('Skipping change - paused or no fullDocument', {
						partition,
					})
					return
				}

				await this.processMessage(change, partition)
			})

			this.setupPartitionEventHandlers(changeStream, partition)
			this.changeStreams.set(partition, changeStream)

			this.logger.info('Change stream created successfully', { partition })
		} catch (error) {
			this.logger.error('Failed to create change stream', {
				partition,
				error: error,
			})
			throw error
		}
	}

	private setupPartitionEventHandlers(
		changeStream: ChangeStream,
		partition: number,
	): void {
		// Handler de erro
		changeStream.on('error', async (error: Error) => {
			try {
				this.logger.error('Change stream error:', {
					message: error.message,
					stack: error.stack,
					topic: this.config.topic,
					partition: partition,
					consumerId: this.consumerId,
				})

				if (!this.currentHandlerConfig) {
					throw new Error('No handler config for error handling')
				}

				if (this.currentHandlerConfig.errorHandler) {
					await this.currentHandlerConfig.errorHandler(error)
				}
			} catch (handlerError) {
				this.logger.error('Error in error handler:', handlerError)
			} finally {
				await this.reconnectPartition(partition)
			}
		})

		// Handler de fechamento
		changeStream.on('close', async () => {
			this.logger.info('Change stream closed', {
				topic: this.config.topic,
				partition: partition,
				consumerId: this.consumerId,
			})

			await this.reconnectPartition(partition)
		})

		// Handler de fim de stream
		changeStream.on('end', async () => {
			this.logger.info('Change stream ended', {
				topic: this.config.topic,
				partition: partition,
				consumerId: this.consumerId,
			})

			await this.reconnectPartition(partition)
		})

		// Handler de timeout (se aplicável)
		changeStream.on('timeout', async () => {
			this.logger.warn('Change stream timeout', {
				topic: this.config.topic,
				partition: partition,
				consumerId: this.consumerId,
			})

			await this.reconnectPartition(partition)
		})
	}

	// Método para reconectar uma partição específica
	private async reconnectPartition(partition: number): Promise<void> {
		const backoff = this.partitionBackoffs.get(partition)
		const isReconnecting = this.partitionReconnecting.get(partition)

		if (!backoff || isReconnecting) {
			this.logger.info('Reconnection already in progress or no backoff', {
				partition,
			})
			return
		}

		this.partitionReconnecting.set(partition, true)

		try {
			if (!backoff.shouldRetry()) {
				this.logger.error('Max reconnection attempts exceeded for partition', {
					partition,
					attempt: backoff.getAttempt(),
				})
				return
			}

			const delay = backoff.getNextDelay()

			this.logger.info('Reconnecting change stream', {
				partition,
				delayMs: delay,
				attempt: backoff.getAttempt(),
			})

			// Fechar stream atual
			const currentStream = this.changeStreams.get(partition)
			if (currentStream) {
				try {
					await currentStream.close()
				} catch (closeError) {
					this.logger.warn('Error closing change stream:', closeError)
				}
				this.changeStreams.delete(partition)
			}

			// Esperar o delay
			await new Promise((resolve) => setTimeout(resolve, delay))

			// Recriar stream
			await this.createPartitionStream(partition)

			// Reset backoff apenas para esta partição
			backoff.reset()
		} catch (error) {
			this.logger.error('Partition reconnection failed:', { partition, error })

			// Tentar reconectar novamente
			if (backoff.shouldRetry()) {
				await this.reconnectPartition(partition)
			}
		} finally {
			this.partitionReconnecting.set(partition, false)
		}
	}

	private async processMessage<T extends Document = Document>(
		change: ChangeStreamEvent<T>,
		partition: number,
	): Promise<void> {
		try {
			if (!change.fullDocument) {
				this.logger.warn('Received change event without fullDocument')
				return
			}

			this.logger.info('🟢 PROCESSING MESSAGE', {
				partition,
				operationType: change.operationType,
				documentId: change.fullDocument._id,
				offset: change._id,
			})

			const record: ConsumerRecord<T> = {
				topic: this.config.topic,
				partition: partition,
				message: {
					value: change.fullDocument,
					timestamp: change.clusterTime || new Date(),
				},
				offset: change._id,
				timestamp: new Date(),
			}

			// CORREÇÃO: Usar this.currentHandlerConfig em vez de parâmetro
			if (!this.currentHandlerConfig) {
				throw new Error('No handler configuration available')
			}

			// Type assertion para o tipo específico
			const config = this.getHandlerConfig<T>()
			await config.handler(record)

			this.partitionOffsets.set(partition, change._id)
			this.partitionUncommittedChanges.set(partition, true)

			this.logger.info('📝 OFFSET UPDATED', {
				partition,
				offset: change._id,
				hasUncommitted: true,
			})

			this.consumerGroupManager.updateOffset(
				this.config.groupId,
				this.config.topic,
				change._id,
				partition,
			)

			this.retryCount = 0

			if (config.autoCommit === true) {
				await this.commitPartitionOffset(partition)
			}
		} catch (error) {
			await this.handleProcessingError(
				error as Error,
				this.createConsumerRecordFromChange(change),
			)
		}
	}

	private createConsumerRecordFromChange<T extends Document = Document>(
		change: ChangeStreamEvent<T>,
	): ConsumerRecord<T> | null {
		if (!change.fullDocument) {
			return null
		}

		return {
			topic: this.config.topic,
			partition: 0,
			message: {
				value: change.fullDocument,
				timestamp: change.clusterTime || new Date(),
				headers: {
					'operation-type': change.operationType,
					'cluster-time': change.clusterTime?.toString(),
					'document-key': JSON.stringify(change.documentKey),
				},
			},
			offset: change._id,
			timestamp: new Date(),
		}
	}

	private isHandlerConfigValid<T extends Document = Document>(
		config: any,
	): config is MessageHandlerConfig<T> {
		return (
			config &&
			typeof config.handler === 'function' &&
			(config.errorHandler === undefined ||
				typeof config.errorHandler === 'function') &&
			(config.maxRetries === undefined ||
				typeof config.maxRetries === 'number') &&
			(config.retryDelay === undefined ||
				typeof config.retryDelay === 'number') &&
			(config.autoCommit === undefined ||
				typeof config.autoCommit === 'boolean')
		)
	}

	private getHandlerConfig<
		T extends Document = Document,
	>(): MessageHandlerConfig<T> {
		if (!this.currentHandlerConfig) {
			throw new Error('No handler configuration available')
		}

		if (!this.isHandlerConfigValid<T>(this.currentHandlerConfig)) {
			throw new Error('Invalid handler configuration')
		}

		return this.currentHandlerConfig as MessageHandlerConfig<T>
	}

	private async handleProcessingError<T extends Document = Document>(
		error: Error,
		record: ConsumerRecord<T> | null,
	): Promise<void> {
		this.retryCount++

		// CORREÇÃO: Usar this.currentHandlerConfig
		if (!this.currentHandlerConfig) {
			this.logger.error(
				'Error processing message, but no handler config available:',
				error,
			)
			return
		}

		const config = this.currentHandlerConfig as MessageHandlerConfig<T>

		if (config.errorHandler) {
			await config.errorHandler(error, record)
		} else {
			this.logger.error(`Error processing message: ${error.message}`, {
				record,
				changeId: record?.offset,
			})
		}

		if (this.retryCount >= (config.maxRetries || 3)) {
			this.logger.error(
				`Max retries (${config.maxRetries || 3}) exceeded for message`,
			)
			this.retryCount = 0
		}
	}

	private setupAutoCommit(): void {
		if (this.config.autoCommit && this.config.autoCommitIntervalMs) {
			this.commitTimer = setInterval(async () => {
				if (this.hasUncommittedChanges) {
					const committed = await this.commitOffsets()

					if (committed) {
						this.logger.info('Auto-commit completed successfully')
					}
				} else {
					this.logger.info('No uncommitted changes, skipping auto-commit')
				}
			}, this.config.autoCommitIntervalMs)
		}
	}

	async unsubscribe(): Promise<void> {
		// Fechar todos os streams de partição
		for (const [partition, stream] of this.changeStreams) {
			try {
				await stream.close()
				this.logger.info(`Closed stream for partition ${partition}`)
			} catch (error) {
				this.logger.warn(
					`Error closing stream for partition ${partition}:`,
					error,
				)
			}
		}
		this.changeStreams.clear()

		if (this.commitTimer) {
			clearInterval(this.commitTimer)
			this.commitTimer = null
		}

		this.currentHandlerConfig = null
		this.logger.info(
			`Consumer ${this.consumerId} unsubscribed from topic ${this.config.topic}`,
		)
	}

	pause(): void {
		this.isPaused = true
		this.logger.info(`Consumer ${this.consumerId} paused`)
	}

	resume(): void {
		this.isPaused = false
		this.logger.info(`Consumer ${this.consumerId} resumed`)
	}

	private isSameOffset(offset1: ResumeToken, offset2: ResumeToken): boolean {
		if (!offset1 || !offset2) return false
		return JSON.stringify(offset1) === JSON.stringify(offset2)
	}

	private async commitPartitionOffset(partition: number): Promise<boolean> {
		const offset = this.partitionOffsets.get(partition)

		this.logger.info('💾 ATTEMPTING COMMIT', {
			partition,
			hasOffset: !!offset,
			offset: offset,
		})

		if (!offset) {
			this.logger.warn('💾 NO OFFSET TO COMMIT', { partition })
			return false
		}

		const lastCommitted = this.partitionCommittedOffsets.get(partition)
		if (lastCommitted && this.isSameOffset(lastCommitted, offset)) {
			this.logger.info('💾 OFFSET ALREADY COMMITTED', { partition })
			this.partitionUncommittedChanges.set(partition, false)
			return false
		}

		try {
			const committed = await this.offsetStorage.commitOffsetIfChanged({
				topic: this.config.topic,
				partition: partition,
				groupId: this.config.groupId,
				offset: offset,
				timestamp: new Date(),
			})

			if (committed) {
				this.partitionCommittedOffsets.set(partition, offset)
				this.partitionUncommittedChanges.set(partition, false)

				this.logger.info('💾 COMMIT SUCCESSFUL', {
					partition,
					offset: offset,
				})

				return true
			} else {
				this.logger.info('💾 COMMIT SKIPPED (no changes)', { partition })
				return false
			}
		} catch (error) {
			if (error instanceof Error) {
				this.logger.error('💾 COMMIT FAILED', {
					partition,
					error: error.message,
					offset: offset,
				})
			} else {
				this.logger.error('💾 COMMIT FAILED', {
					partition,
					error: error,
					offset: offset,
				})
			}
			return false
		}
	}

	async commitOffsets(): Promise<boolean> {
		let anyCommitted = false

		for (const partition of this.config.partitions) {
			if (this.partitionUncommittedChanges.get(partition)) {
				const committed = await this.commitPartitionOffset(partition)
				anyCommitted = anyCommitted || committed
			}
		}

		return anyCommitted
	}

	async disconnect(): Promise<void> {
		// Commit final se houver mudanças não commitadas
		if (this.hasUncommittedChangesOnPartition()) {
			this.logger.info('Performing final commit before disconnect')
			await this.commitOffsets()
		}

		await this.unsubscribe()

		this.consumerGroupManager.removeMemberFromGroup(
			this.config.groupId,
			this.consumerId,
		)

		if (this.client) {
			await this.client.close()
			this.client = null
		}

		// Limpar todas as estruturas de partição
		this.partitionOffsets.clear()
		this.partitionCommittedOffsets.clear()
		this.partitionUncommittedChanges.clear()

		this.isRunning = false
		this.currentHandlerConfig = null

		this.logger.info(`Consumer ${this.consumerId} disconnected gracefully`)
	}

	private hasUncommittedChangesOnPartition(): boolean {
		return Array.from(this.partitionUncommittedChanges.values()).some(
			(hasChanges) => hasChanges,
		)
	}

	private async processExistingDocuments(
		collection: Collection,
		partition: number,
	): Promise<void> {
		try {
			this.logger.info(
				'🟢 Processing existing documents for fromBeginning strategy',
				{
					partition,
					collection: collection.collectionName,
				},
			)

			const documents = await collection.find({}).sort({ _id: 1 }).toArray()
			let processedCount = 0

			for (const document of documents) {
				// CORREÇÃO: Usar a mesma lógica do processMessage mas para documentos estáticos
				const fakeChangeEvent: ChangeStreamEvent<Document> = {
					_id: { _data: `historical_${document._id}` } as ResumeToken,
					operationType: 'insert' as const, // Ou 'historical'
					clusterTime: new Date(),
					fullDocument: document,
					documentKey: { _id: document._id },
					ns: {
						db: this.database,
						coll: collection.collectionName,
					},
				}

				// Reutilizar a lógica existente do processMessage
				await this.processMessage(fakeChangeEvent, partition)

				processedCount++

				if (processedCount % 100 === 0) {
					this.logger.info(
						`🟢 Processed ${processedCount} existing documents`,
						{
							partition,
						},
					)
				}
			}

			this.logger.info('🟢 Finished processing existing documents', {
				partition,
				totalProcessed: processedCount,
			})
		} catch (error) {
			if (error instanceof Error) {
				this.logger.error('🔴 Error processing existing documents', {
					partition,
					error: error.message,
				})
			} else {
				this.logger.error('🔴 Unknown error processing existing documents', {
					partition,
					error: error,
				})
			}

			throw error
		}
	}

	debugOffsets() {
		const state = {
			consumerId: this.consumerId,
			topic: this.config.topic,
			groupId: this.config.groupId,
			partitions: this.config.partitions.map((partition) => ({
				partition,
				processedOffset: this.partitionOffsets.get(partition),
				committedOffset: this.partitionCommittedOffsets.get(partition),
				hasUncommitted: this.partitionUncommittedChanges.get(partition),
				isReconnecting: this.partitionReconnecting.get(partition),
			})),
			summary: {
				totalProcessedOffsets: Array.from(
					this.partitionOffsets.values(),
				).filter(Boolean).length,
				totalCommittedOffsets: Array.from(
					this.partitionCommittedOffsets.values(),
				).filter(Boolean).length,
				totalUncommitted: Array.from(
					this.partitionUncommittedChanges.values(),
				).filter(Boolean).length,
			},
		}

		this.logger.info('🔍 OFFSET DEBUG STATE:', state)
		return state
	}
}
