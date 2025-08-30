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
} from '../types/types'
import { Logger } from '../utils/logger'

export class TopicManager {
	private client!: MongoClient
	private db!: Db
	private topicsCollection!: Collection<TopicConfig & Document>

	constructor(
		private mongoUri: string,
		private database: string = 'change-stream-broker',
	) {}

	async connect(): Promise<void> {
		this.client = new MongoClient(this.mongoUri)
		await this.client.connect()
		this.db = this.client.db(this.database)
		this.topicsCollection = this.db.collection<TopicConfig & Document>('topics')

		Logger.info('TopicManager connected to MongoDB')
	}

	private async createTTLIndex(
		collectionName: string,
		retentionMs: number,
	): Promise<IndexOperationResult> {
		try {
			const collection = this.db.collection(collectionName)

			// Verificar se o index já existe usando nossa função que retorna o objeto completo
			const existingIndexInfo = await this.getTTLIndexInfo(collectionName)

			if (existingIndexInfo) {
				Logger.info(`TTL index already exists for collection ${collectionName}`)
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

			Logger.info(`TTL index created for collection ${collectionName}`, {
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
				Logger.warn(
					`TTL index conflict for ${collectionName}: ${error.message}`,
				)
				return {
					success: false,
					message: `TTL index conflict: ${error.message}`,
				}
			}

			Logger.error(`Failed to create TTL index for ${collectionName}:`, error)
			return {
				success: false,
				message: `Failed to create TTL index: ${JSON.stringify(error)}`,
			}
		}
	}

	async getTTLIndexInfo(collectionName: string): Promise<TTLIndexInfo | null> {
		try {
			const collection = this.db.collection(collectionName)
			const indexes = await collection.indexes()

			const ttlIndex = indexes.find(
				(index) =>
					index.name === `ttl_${collectionName}_timestamp` &&
					index.expireAfterSeconds !== undefined,
			)

			if (!ttlIndex) {
				return null
			}

			// Type assertion para garantir a tipagem correta
			return ttlIndex as TTLIndexInfo
		} catch (error) {
			Logger.error(`Failed to get TTL index info for ${collectionName}:`, error)
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
			const expireAfterSeconds = Math.floor(retentionMs / 1000)

			// Dropar o index existente
			await collection.dropIndex(`ttl_${collectionName}_timestamp`)

			// Criar novo index
			const createResult = await this.createTTLIndex(
				collectionName,
				expireAfterSeconds,
			)

			if (!createResult.success) {
				return createResult
			}

			return {
				success: true,
				message: 'TTL index updated successfully',
				indexInfo: createResult.indexInfo, // Já é TTLIndexInfo | undefined
			}
		} catch (error) {
			Logger.error(`Failed to update TTL index for ${collectionName}:`, error)
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

		const now = new Date()
		const existingTopic = (await this.topicsCollection.findOne({
			name: config.name,
		})) as TopicDocumentWithId | null

		const topicDoc: OptionalUnlessRequiredId<TopicDocument> = {
			name: config.name,
			collection: config.collection,
			partitions: config.partitions,
			retentionMs: config.retentionMs,
			createdAt: existingTopic?.createdAt || now,
			updatedAt: now,
		}

		await this.topicsCollection.updateOne(
			{ name: config.name },
			{
				$set: topicDoc,
				$setOnInsert: {
					createdAt: now,
				},
			},
			{ upsert: true },
		)

		// Gerenciar TTL index apenas se retentionMs for fornecido
		if (config.retentionMs !== undefined) {
			if (existingTopic?.retentionMs !== config.retentionMs) {
				// Retention mudou, atualizar index
				await this.updateTTLIndex(config.collection, config.retentionMs)
			} else if (!existingTopic) {
				// Novo tópico, criar index
				await this.createTTLIndex(config.collection, config.retentionMs)
			}
		} else if (existingTopic?.retentionMs !== undefined) {
			// Retention foi removida, dropar index
			await this.removeTTLIndex(config.collection)
		}

		Logger.info(`Topic ${config.name} created/updated`)
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
			const collection = this.db.collection(collectionName)
			await collection.dropIndex(`ttl_${collectionName}_timestamp`)

			Logger.info(`TTL index removed from collection ${collectionName}`)
			return this.createIndexOperationResult(
				true,
				'TTL index removed successfully',
			)
		} catch (error) {
			if (
				error instanceof MongoServerError &&
				error.codeName === 'IndexNotFound'
			) {
				Logger.info(
					`TTL index not found for ${collectionName}, nothing to remove`,
				)
				return this.createIndexOperationResult(
					true,
					'TTL index not found, nothing to remove',
				)
			}

			Logger.error(`Failed to remove TTL index from ${collectionName}:`, error)
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
			Logger.info('TopicManager disconnected')
		}
	}

	// Método para verificar se está conectado
	isConnected(): boolean {
		return !!this.client && !!this.db && !!this.topicsCollection
	}
}
