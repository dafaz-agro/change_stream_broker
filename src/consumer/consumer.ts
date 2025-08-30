import { ChangeStream, Document, MongoClient, ResumeToken } from 'mongodb'
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
	private backoff: BackoffManager
	private lastProcessedOffset: ResumeToken | null = null
	private commitTimer: NodeJS.Timeout | null = null
	private consumerId: string
	private retryCount = 0
	private currentHandlerConfig: MessageHandlerConfig<Document> | null = null
	private isReconnecting = false

	constructor(
		private config: ConsumerConfig,
		private offsetStorage: OffsetStorage,
		private consumerGroupManager: ConsumerGroupManager,
		private mongoUri: string,
		private database: string,
	) {
		this.backoff = new BackoffManager(
			config.maxRetries || 10,
			config.retryDelayMs || 1000,
		)
		this.consumerId = this.generateConsumerId()
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

		Logger.info(
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

		const groupOffset = this.consumerGroupManager.getOffset(
			this.config.groupId,
			this.config.topic,
		)

		const lastOffset =
			groupOffset ||
			(await this.offsetStorage.getOffset(
				this.config.groupId,
				this.config.topic,
				0,
			))

		if (!this.client) {
			throw new Error('MongoClient is not initialized')
		}

		const db = this.client.db(this.database)
		const collection = db.collection(this.config.topic)

		const options: ChangeStreamWatchOptions = {
			fullDocument: 'updateLookup' as const,
			batchSize: this.config.options?.batchSize || 100,
			maxAwaitTimeMS: this.config.options?.maxAwaitTimeMS || 1000,
		}

		if (lastOffset && !this.config.fromBeginning) {
			options.resumeAfter = lastOffset
		}

		if (this.config.options) {
			Object.assign(options, this.config.options)
			if (this.config.options.fullDocument) {
				options.fullDocument = this.config.options.fullDocument
			}
		}

		this.changeStream = collection.watch([], options)

		// CORREÇÃO: Handler simplificado sem parâmetro config
		this.changeStream.on(
			'change',
			async (change: ChangeStreamEvent<Document>) => {
				if (this.isPaused || !change.fullDocument) {
					return
				}

				await this.processMessage(change)
			},
		)

		this.setupEventHandlers()
		this.setupAutoCommit()

		Logger.info(
			`Consumer ${this.consumerId} subscribed to topic ${this.config.topic}`,
		)
	}

	private async processMessage<T extends Document = Document>(
		change: ChangeStreamEvent<T>,
	): Promise<void> {
		try {
			if (!change.fullDocument) {
				Logger.warn('Received change event without fullDocument')
				return
			}

			const record: ConsumerRecord<T> = {
				topic: this.config.topic,
				partition: 0,
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

			this.lastProcessedOffset = change._id
			this.consumerGroupManager.updateOffset(
				this.config.groupId,
				this.config.topic,
				change._id,
			)

			this.retryCount = 0

			if (config.autoCommit !== false) {
				await this.commitOffsets()
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
			Logger.error(
				'Error processing message, but no handler config available:',
				error,
			)
			return
		}

		const config = this.currentHandlerConfig as MessageHandlerConfig<T>

		if (config.errorHandler) {
			await config.errorHandler(error, record)
		} else {
			Logger.error(`Error processing message: ${error.message}`, {
				record,
				changeId: record?.offset,
			})
		}

		if (this.retryCount >= (config.maxRetries || 3)) {
			Logger.error(
				`Max retries (${config.maxRetries || 3}) exceeded for message`,
			)
			this.retryCount = 0
		}
	}

	private setupEventHandlers(): void {
		if (!this.changeStream) return

		// Handler de erro
		this.changeStream.on('error', async (error: Error) => {
			try {
				Logger.error('Change stream error:', {
					message: error.message,
					stack: error.stack,
					topic: this.config.topic,
					consumerId: this.consumerId,
				})

				if (!this.currentHandlerConfig) {
					throw new Error('No handler config for error handling')
				}

				if (this.currentHandlerConfig.errorHandler) {
					await this.currentHandlerConfig.errorHandler(error)
				}
			} catch (handlerError) {
				Logger.error('Error in error handler:', handlerError)
			} finally {
				await this.reconnect()
			}
		})

		// Handler de fechamento
		this.changeStream.on('close', async () => {
			Logger.info('Change stream closed', {
				topic: this.config.topic,
				consumerId: this.consumerId,
			})

			await this.reconnect()
		})

		// Handler de fim de stream
		this.changeStream.on('end', async () => {
			Logger.info('Change stream ended', {
				topic: this.config.topic,
				consumerId: this.consumerId,
			})

			await this.reconnect()
		})

		// Handler de timeout (se aplicável)
		this.changeStream.on('timeout', async () => {
			Logger.warn('Change stream timeout', {
				topic: this.config.topic,
				consumerId: this.consumerId,
			})

			await this.reconnect()
		})
	}

	private setupAutoCommit(): void {
		if (this.config.autoCommit && this.config.autoCommitIntervalMs) {
			this.commitTimer = setInterval(() => {
				this.commitOffsets().catch((error) => {
					Logger.error('Auto commit failed:', error)
				})
			}, this.config.autoCommitIntervalMs)
		}
	}

	private async reconnect(): Promise<void> {
		// Prevenir múltiplas reconexões simultâneas
		if (this.isReconnecting) {
			Logger.info('Reconnection already in progress', {
				topic: this.config.topic,
				consumerId: this.consumerId,
			})
			return
		}

		this.isReconnecting = true

		try {
			if (!this.backoff.shouldRetry()) {
				Logger.error('Max reconnection attempts exceeded', {
					topic: this.config.topic,
					consumerId: this.consumerId,
					retryCount: this.backoff.getAttempt(),
				})
				return
			}

			const delay = this.backoff.getNextDelay()

			Logger.info('Reconnecting change stream', {
				topic: this.config.topic,
				consumerId: this.consumerId,
				delayMs: delay,
				attempt: this.backoff.getAttempt(),
			})

			// Fechar stream atual
			if (this.changeStream) {
				try {
					await this.changeStream.close()
				} catch (closeError) {
					Logger.warn('Error closing change stream:', closeError)
				}
				this.changeStream = null
			}

			// Esperar o delay
			await new Promise((resolve) => setTimeout(resolve, delay))

			// Reassinar usando currentHandlerConfig
			if (this.currentHandlerConfig) {
				await this.subscribe(this.currentHandlerConfig)
			} else {
				Logger.error('Cannot reconnect: no handler configuration available')
			}

			// Reset backoff em caso de sucesso
			this.backoff.reset()
		} catch (error) {
			Logger.error('Reconnection failed:', error)

			// Tentar reconectar novamente em caso de falha
			if (this.backoff.shouldRetry()) {
				await this.reconnect()
			}
		} finally {
			this.isReconnecting = false
		}
	}

	async unsubscribe(): Promise<void> {
		if (this.changeStream) {
			await this.changeStream.close()
			this.changeStream = null
		}

		if (this.commitTimer) {
			clearInterval(this.commitTimer)
			this.commitTimer = null
		}

		this.currentHandlerConfig = null
		Logger.info(
			`Consumer ${this.consumerId} unsubscribed from topic ${this.config.topic}`,
		)
	}

	pause(): void {
		this.isPaused = true
		Logger.info(`Consumer ${this.consumerId} paused`)
	}

	resume(): void {
		this.isPaused = false
		Logger.info(`Consumer ${this.consumerId} resumed`)
	}

	async commitOffsets(): Promise<void> {
		if (this.lastProcessedOffset) {
			await this.offsetStorage.commitOffset({
				topic: this.config.topic,
				partition: 0,
				groupId: this.config.groupId,
				offset: this.lastProcessedOffset,
				timestamp: new Date(),
			})
		}
	}

	async disconnect(): Promise<void> {
		await this.unsubscribe()

		this.consumerGroupManager.removeMemberFromGroup(
			this.config.groupId,
			this.consumerId,
		)

		if (this.client) {
			await this.client.close()
			this.client = null
		}

		this.isRunning = false

		this.currentHandlerConfig = null

		Logger.info(`Consumer ${this.consumerId} disconnected`)
	}
}
