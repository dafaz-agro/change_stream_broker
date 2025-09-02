import {
	Collection,
	Db,
	MongoClient,
	MongoServerError,
	OptionalUnlessRequiredId,
} from 'mongodb'
import {
	IndexOperationResult,
	TopicConfig,
	TopicDocument,
	TopicDocumentWithId,
	TTLIndexInfo,
	TTLValidationResult,
} from '../types'
import { Logger } from '../utils/logger'

export class TopicManager {
	private client!: MongoClient
	private db!: Db
	private topicsCollection!: Collection<TopicConfig & Document>
	private logger: typeof Logger

	constructor(
		private mongoUri: string,
		private database: string = 'change-stream-broker',
	) {
		this.logger = Logger.withContext('TopicManager')
	}

	async connect(): Promise<void> {
		this.client = new MongoClient(this.mongoUri)
		await this.client.connect()
		this.db = this.client.db(this.database)
		this.topicsCollection = this.db.collection<TopicConfig & Document>('topics')

		this.logger.info('TopicManager connected to MongoDB')
	}

	private async createTTLIndex(
		collectionName: string,
		retentionMs: number,
	): Promise<IndexOperationResult> {
		try {
			const collection = this.db.collection(collectionName)

			const collections = await this.db
				.listCollections({
					name: collectionName,
				})
				.toArray()

			if (collections.length === 0) {
				this.logger.info(
					`Collection ${collectionName} does not exist, creating implicitly`,
				)
				await collection.insertOne({
					_timestamp: new Date(),
					_temp: true,
				})
				await collection.deleteOne({ _temp: true })
			}

			// Verificar se o index já existe usando nossa função que retorna o objeto completo
			const existingIndexInfo = await this.getTTLIndexInfo(collectionName)

			if (existingIndexInfo) {
				this.logger.info(
					`TTL index already exists for collection ${collectionName}`,
				)
				return {
					success: true,
					message: 'TTL index already exists',
					indexInfo: existingIndexInfo, // Já temos a informação aqui
				}
			}

			// Converter retentionMs para segundos
			const expireAfterSeconds = Math.floor(retentionMs / 1000)

			// Validar conforme documentação
			if (expireAfterSeconds < 0 || expireAfterSeconds > 2147483641) {
				throw new Error(
					`expireAfterSeconds must be between 0 and 2147483641, got ${expireAfterSeconds}`,
				)
			}

			await collection.createIndex(
				{ _timestamp: 1 },
				{
					expireAfterSeconds,
					name: `ttl_${collectionName}_timestamp`,
					background: true,
				},
			)

			this.logger.info(`TTL index created for collection ${collectionName}`, {
				expireAfterSeconds,
				retentionHours: Math.round(expireAfterSeconds / 3600),
				retentionDays: Math.round(expireAfterSeconds / 86400),
			})

			// Buscar a informação do index recém-criado
			const newIndexInfo = await this.getTTLIndexInfo(collectionName)

			if (newIndexInfo) {
				return {
					success: true,
					message: 'TTL index created successfully',
					indexInfo: newIndexInfo,
				}
			} else {
				return {
					success: true,
					message:
						'TTL index created successfully but could not retrieve index info',
				}
			}
		} catch (error) {
			if (
				error instanceof MongoServerError &&
				error.codeName === 'IndexOptionsConflict'
			) {
				this.logger.warn(
					`TTL index conflict for ${collectionName}: ${error.message}`,
				)
				return {
					success: false,
					message: `TTL index conflict: ${error.message}`,
				}
			}

			this.logger.error(
				`Failed to create TTL index for ${collectionName}:`,
				error,
			)
			return {
				success: false,
				message: `Failed to create TTL index: ${JSON.stringify(error)}`,
			}
		}
	}

	async getTTLIndexInfo(collectionName: string): Promise<TTLIndexInfo | null> {
		try {
			const collection = this.db.collection(collectionName)

			// Verificar se a collection existe primeiro
			const collections = await this.db
				.listCollections({
					name: collectionName,
				})
				.toArray()

			if (collections.length === 0) {
				return null // Collection não existe, logo não há índice
			}

			const indexes = await collection.indexes()

			const ttlIndex = indexes.find(
				(index) =>
					index.name === `ttl_${collectionName}_timestamp` &&
					index.expireAfterSeconds !== undefined,
			)

			return ttlIndex ? (ttlIndex as TTLIndexInfo) : null
		} catch (error) {
			if (error instanceof MongoServerError && error.code === 26) {
				// NamespaceNotFound - collection não existe
				return null
			}

			this.logger.error(
				`Failed to get TTL index info for ${collectionName}:`,
				error,
			)
			return null
		}
	}

	async getTTLIndexStats(): Promise<
		Array<{
			collection: string
			indexInfo?: TTLIndexInfo | undefined // ← Adicionar explicitamente undefined
			documentCount: number
			hasTimestampField: boolean
		}>
	> {
		const stats: Array<{
			collection: string
			indexInfo?: TTLIndexInfo | undefined
			documentCount: number
			hasTimestampField: boolean
		}> = []

		const collections = await this.db.listCollections().toArray()

		for (const collectionInfo of collections) {
			const collection = this.db.collection(collectionInfo.name)
			const ttlIndex = await this.getTTLIndexInfo(collectionInfo.name)

			const documentCount = await collection.countDocuments()
			const hasTimestampField =
				(await collection.countDocuments({
					_timestamp: { $exists: true, $type: 'date' },
				})) > 0

			// CORREÇÃO: Criar objeto com propriedade indexInfo apenas se existir
			const statEntry: {
				collection: string
				indexInfo?: TTLIndexInfo | undefined
				documentCount: number
				hasTimestampField: boolean
			} = {
				collection: collectionInfo.name,
				documentCount,
				hasTimestampField,
			}

			// Adicionar indexInfo apenas se não for null/undefined
			if (ttlIndex) {
				statEntry.indexInfo = ttlIndex
			}

			stats.push(statEntry)
		}

		return stats
	}

	private async updateTTLIndex(
		collectionName: string,
		retentionMs: number,
	): Promise<IndexOperationResult> {
		try {
			const collection = this.db.collection(collectionName)

			// Verificar se collection existe antes de tentar dropar índice
			const collections = await this.db
				.listCollections({
					name: collectionName,
				})
				.toArray()

			if (collections.length > 0) {
				// Collection existe, tentar dropar o índice se existir
				try {
					await collection.dropIndex(`ttl_${collectionName}_timestamp`)
				} catch (dropError) {
					if (
						dropError instanceof MongoServerError &&
						dropError.codeName === 'IndexNotFound'
					) {
						// Índice não existe, não é erro
						this.logger.info(
							`TTL index not found for ${collectionName}, skipping drop`,
						)
					} else {
						throw dropError
					}
				}
			}

			// Criar collection implicitamente inserindo um documento temporário
			// e depois removendo-o (se collection não existir)
			if (collections.length === 0) {
				this.logger.info(
					`Collection ${collectionName} does not exist, creating implicitly`,
				)
				await collection.insertOne({
					_timestamp: new Date(),
					_temp: true,
				})
				await collection.deleteOne({ _temp: true })
			}

			// Agora criar o índice TTL
			const createResult = await this.createTTLIndex(
				collectionName,
				retentionMs,
			)

			if (!createResult.success) {
				return createResult
			}

			return {
				success: true,
				message: 'TTL index updated successfully',
				indexInfo: createResult.indexInfo,
			}
		} catch (error) {
			if (error instanceof MongoServerError && error.code === 26) {
				// NamespaceNotFound - tratar gracefulmente
				return {
					success: false,
					message: `Collection ${collectionName} does not exist and could not be created`,
				}
			}

			this.logger.error(
				`Failed to update TTL index for ${collectionName}:`,
				error,
			)
			return {
				success: false,
				message: `Failed to update TTL index: ${JSON.stringify(error)}`,
			}
		}
	}

	async createTopic(config: TopicConfig): Promise<void> {
		if (!this.topicsCollection) {
			throw new Error('TopicManager not connected. Call connect() first.')
		}

		console.log(`Configuração do Tópico ${JSON.stringify(config)}`)

		const now = new Date()
		const existingTopic = (await this.topicsCollection.findOne({
			name: config.name,
		})) as TopicDocumentWithId | null

		// Preparar operação de update sem conflitos
		const updateFields: Partial<OptionalUnlessRequiredId<TopicDocument>> = {
			name: config.name,
			collection: config.collection,
			partitions: config.partitions,
			retentionMs: config.retentionMs,
			updatedAt: now,
		}

		// Se for novo tópico, adicionar createdAt via $setOnInsert
		const updateOperation: Partial<OptionalUnlessRequiredId<TopicDocument>> = {
			$set: updateFields,
		}

		if (!existingTopic) {
			updateOperation.$setOnInsert = {
				createdAt: now,
			}
		}

		await this.topicsCollection.updateOne(
			{ name: config.name },
			updateOperation,
			{ upsert: true },
		)

		// LÓGICA CORRIGIDA PARA MULTIPLAS PARTIÇÕES
		for (let partition = 0; partition < config.partitions; partition++) {
			const collectionName = this.getPartitionCollectionName(
				config.collection,
				partition,
			)

			if (config.retentionMs !== undefined) {
				// CASO 1: Retention está definida (criar/atualizar TTL)
				const hasExistingIndex = !!(await this.getTTLIndexInfo(collectionName))

				if (existingTopic?.retentionMs !== config.retentionMs) {
					// Retention mudou, atualizar index
					await this.updateTTLIndex(collectionName, config.retentionMs)
				} else if (!hasExistingIndex) {
					// Novo tópico OU index não existe, criar index
					console.log(`Criando index TTL para ${collectionName}`)
					await this.createTTLIndex(collectionName, config.retentionMs)
				}
				// Se retention é igual e index já existe, não faz nada
			} else if (existingTopic?.retentionMs !== undefined) {
				// CASO 2: Retention foi REMOVIDA (config.retentionMs é undefined)
				// e antes existia uma retention → remover TTL index
				console.log(
					`Removendo TTL index de ${collectionName} (retention removida)`,
				)
				await this.removeTTLIndex(collectionName)
			}
			// CASO 3: retentionMs é undefined E não existia antes → não faz nada
		}

		this.logger.info(`Topic ${config.name} created/updated`)
	}

	// Adicionar método auxiliar para nomes de partição
	private getPartitionCollectionName(
		baseCollection: string,
		partition: number,
	): string {
		return `${baseCollection}_p${partition}`
	}

	private createIndexOperationResult(
		success: boolean,
		message: string,
		indexInfo?: TTLIndexInfo,
	): IndexOperationResult {
		return {
			success,
			message,
			indexInfo: indexInfo || undefined, // Garante que seja undefined ao invés de null
		}
	}

	private async removeTTLIndex(
		collectionName: string,
	): Promise<IndexOperationResult> {
		try {
			// Verificar se collection existe primeiro
			const collections = await this.db
				.listCollections({
					name: collectionName,
				})
				.toArray()

			if (collections.length === 0) {
				this.logger.info(
					`Collection ${collectionName} does not exist, nothing to remove`,
				)
				return this.createIndexOperationResult(
					true,
					'Collection does not exist, nothing to remove',
				)
			}

			const collection = this.db.collection(collectionName)

			try {
				await collection.dropIndex(`ttl_${collectionName}_timestamp`)
				this.logger.info(`TTL index removed from collection ${collectionName}`)
				return this.createIndexOperationResult(
					true,
					'TTL index removed successfully',
				)
			} catch (error) {
				if (
					error instanceof MongoServerError &&
					error.codeName === 'IndexNotFound'
				) {
					this.logger.info(
						`TTL index not found for ${collectionName}, nothing to remove`,
					)
					return this.createIndexOperationResult(
						true,
						'TTL index not found, nothing to remove',
					)
				}
				throw error
			}
		} catch (error) {
			this.logger.error(
				`Failed to remove TTL index from ${collectionName}:`,
				error,
			)
			return this.createIndexOperationResult(
				false,
				`Failed to remove TTL index: ${JSON.stringify(error)}`,
			)
		}
	}

	async topicExists(topicName: string): Promise<boolean> {
		if (!this.topicsCollection) {
			throw new Error('TopicManager not connected. Call connect() first.')
		}

		const topic = (await this.topicsCollection.findOne({
			name: topicName,
		})) as unknown as TopicDocumentWithId | null

		return !!topic
	}

	async validateTTLIndex(collectionName: string): Promise<TTLValidationResult> {
		const issues: string[] = []

		try {
			const collection = this.db.collection(collectionName)

			// Verificar se collection existe
			const collections = await this.db
				.listCollections({ name: collectionName })
				.toArray()
			if (collections.length === 0) {
				issues.push(`Collection ${collectionName} does not exist`)
				return { isValid: false, issues }
			}

			// Verificar se index TTL existe
			const ttlIndex = await this.getTTLIndexInfo(collectionName)
			if (!ttlIndex) {
				issues.push(`TTL index does not exist for collection ${collectionName}`)
				return { isValid: false, issues }
			}

			// Verificar se campo _timestamp existe em alguns documentos
			const sampleDoc = await collection.findOne({
				_timestamp: { $exists: true },
			})
			if (!sampleDoc) {
				issues.push(
					`No documents with _timestamp field found in ${collectionName}`,
				)
			}

			// Verificar se o campo é do tipo Date
			const invalidTypeDoc = await collection.findOne({
				_timestamp: { $exists: true, $not: { $type: 'date' } },
			})

			if (invalidTypeDoc) {
				issues.push(`Found documents with _timestamp field that is not a Date`)
			}

			return {
				isValid: issues.length === 0,
				issues,
				indexInfo: ttlIndex,
				sampleDocument: sampleDoc,
			}
		} catch (error) {
			issues.push(`Validation error: ${error}`)
			return { isValid: false, issues }
		}
	}

	async getTopicConfig(topicName: string): Promise<TopicConfig | null> {
		if (!this.topicsCollection) {
			throw new Error('TopicManager not connected. Call connect() first.')
		}

		const result = await this.topicsCollection.findOne({ name: topicName })

		if (!result) {
			return null
		}

		return {
			name: result.name,
			collection: result.collection,
			partitions: result.partitions,
			retentionMs: result.retentionMs,
		}
	}

	async disconnect(): Promise<void> {
		if (this.client) {
			await this.client.close()
			this.logger.info('TopicManager disconnected')
		}
	}

	// Método para verificar se está conectado
	isConnected(): boolean {
		return !!this.client && !!this.db && !!this.topicsCollection
	}
}
