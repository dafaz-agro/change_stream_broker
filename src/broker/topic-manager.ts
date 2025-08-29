import { Collection, Db, MongoClient, OptionalUnlessRequiredId } from 'mongodb'
import { Logger } from '../utils/logger'
import { TopicConfig, TopicDocument } from './types'

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

	async createTopic(config: TopicConfig): Promise<void> {
		if (!this.topicsCollection) {
			throw new Error('TopicManager not connected. Call connect() first.')
		}

		const now = new Date()

		const topicDoc: OptionalUnlessRequiredId<TopicDocument> = {
			name: config.name,
			collection: config.collection,
			partitions: config.partitions,
			retentionMs: config.retentionMs,
			createdAt: now,
			updatedAt: now,
		}

		await this.topicsCollection.updateOne(
			{ name: config.name },
			{
				$set: {
					...topicDoc,
					updatedAt: now,
				},
				$setOnInsert: {
					createdAt: now,
				},
			},
			{ upsert: true },
		)

		Logger.info(`Topic ${config.name} created/updated`)
	}

	async topicExists(topicName: string): Promise<boolean> {
		if (!this.db) {
			throw new Error('TopicManager not connected. Call connect() first.')
		}

		const topicsCollection = this.db.collection('topics')
		const topic = await topicsCollection.findOne({ name: topicName })
		return !!topic
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
