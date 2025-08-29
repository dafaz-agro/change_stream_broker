import {
	Collection,
	Db,
	Document,
	MongoClient,
	MongoServerError,
	OptionalUnlessRequiredId,
	ResumeToken,
} from 'mongodb'
import {
	OffsetStorage as MongoOffsetStorage,
	OffsetCommit,
} from '../broker/types'
import { Logger } from '../utils/logger'

export interface OffsetDocument extends Document {
	groupId: string
	topic: string
	partition: number
	offset: ResumeToken
	timestamp: Date
}

export class OffsetStorage implements MongoOffsetStorage {
	private client: MongoClient | null = null
	private db: Db | null = null
	private collection: Collection<OffsetDocument> | null = null
	private isConnecting: boolean = false
	private lastConnectionCheck: number = 0
	private connectionStatus: boolean = false

	constructor(
		private mongoUri: string,
		private database: string = 'change-stream-broker',
	) {}

	async connect(): Promise<void> {
		if (this.isConnecting) {
			throw new Error('Connection already in progress')
		}

		if (this.isConnected()) {
			Logger.warn('OffsetStorage already connected')
			return
		}

		this.isConnecting = true

		try {
			this.client = new MongoClient(this.mongoUri, {
				serverSelectionTimeoutMS: 5000,
				connectTimeoutMS: 5000,
			})

			await this.client.connect()

			this.db = this.client.db(this.database)
			this.collection = this.db.collection<OffsetDocument>('consumer_offsets')

			// Criar índice com tratamento de erro
			try {
				await this.collection.createIndex(
					{ groupId: 1, topic: 1, partition: 1 },
					{ unique: true },
				)
			} catch (indexError) {
				if (indexError instanceof MongoServerError && indexError.code === 85) {
					// Código 85: Index already exists with different options
					Logger.warn('Index already exists with different options')
				} else {
					throw indexError
				}
			}

			Logger.info('OffsetStorage connected successfully', {
				database: this.database,
				collection: 'consumer_offsets',
			})
		} catch (error) {
			Logger.error('Failed to connect OffsetStorage:', error)
			await this.cleanup()
			throw error
		} finally {
			this.isConnecting = false
		}
	}

	async ensureConnected(): Promise<void> {
		if (!this.isConnected()) {
			throw new Error('OffsetStorage not connected. Call connect() first.')
		}

		// Verificar conexão ativa a cada 30 segundos
		const now = Date.now()
		if (now - this.lastConnectionCheck > 30000) {
			this.connectionStatus = await this.verifyConnection()
			this.lastConnectionCheck = now
		}

		if (!this.connectionStatus) {
			throw new Error(
				'OffsetStorage connection lost. Attempting to reconnect...',
			)
		}
	}

	private async verifyConnection(): Promise<boolean> {
		if (!this.isConnected()) {
			return false
		}

		try {
			// Comando simples para testar a conexão
			if (!this.db) {
				return false
			}

			await this.db.command({ ping: 1 })
			return true
		} catch (error) {
			Logger.warn('Connection verification failed:', error)
			return false
		}
	}

	async getOffset(
		groupId: string,
		topic: string,
		partition: number,
	): Promise<ResumeToken | null> {
		await this.ensureConnected()

		try {
			if (!this.collection) {
				throw new Error('Collection is not initialized')
			}
			const doc = await this.collection.findOne({
				groupId,
				topic,
				partition,
			})

			return doc?.offset || null
		} catch (error) {
			Logger.error('Error getting offset:', error)
			throw error
		}
	}

	async commitOffset(commit: OffsetCommit): Promise<void> {
		this.ensureConnected()

		try {
			const updateDoc: OptionalUnlessRequiredId<OffsetDocument> = {
				groupId: commit.groupId,
				topic: commit.topic,
				partition: commit.partition,
				offset: commit.offset,
				timestamp: commit.timestamp,
			}

			if (!this.collection) {
				throw new Error('Collection is not initialized')
			}

			await this.collection.updateOne(
				{
					groupId: commit.groupId,
					topic: commit.topic,
					partition: commit.partition,
				},
				{
					$set: updateDoc,
					$setOnInsert: {
						createdAt: new Date(),
					},
				},
				{ upsert: true },
			)

			Logger.info('Offset committed successfully', {
				groupId: commit.groupId,
				topic: commit.topic,
				partition: commit.partition,
			})
		} catch (error) {
			Logger.error('Error committing offset:', error)
			throw error
		}
	}

	async getOffsets(groupId: string): Promise<OffsetCommit[]> {
		this.ensureConnected()

		try {
			if (!this.collection) {
				throw new Error('Collection is not initialized')
			}
			const documents = await this.collection
				.find({ groupId })
				.sort({ timestamp: -1 })
				.toArray()

			return documents.map((doc) => ({
				groupId: doc.groupId,
				topic: doc.topic,
				partition: doc.partition,
				offset: doc.offset,
				timestamp: doc.timestamp,
			}))
		} catch (error) {
			Logger.error('Error getting offsets:', error)
			throw error
		}
	}

	async disconnect(): Promise<void> {
		await this.cleanup()
		Logger.info('OffsetStorage disconnected')
	}

	isConnected(): boolean {
		return this.client !== null && this.collection !== null
	}

	getConnectionInfo(): { database: string; collection: string } {
		return {
			database: this.database,
			collection: 'consumer_offsets',
		}
	}

	private async cleanup(): Promise<void> {
		if (this.client) {
			try {
				await this.client.close()
			} catch (error) {
				Logger.warn('Error closing MongoDB client:', error)
			}
		}

		this.client = null
		this.db = null
		this.collection = null
		this.isConnecting = false
	}
}
