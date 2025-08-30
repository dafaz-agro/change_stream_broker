import { Document, MongoClient, OptionalId } from 'mongodb'
import { IChangeStreamProducer, Message, ProducerConfig } from '../types/types'
import { Logger } from '../utils/logger'

export class ChangeStreamProducer implements IChangeStreamProducer {
	private client: MongoClient | null = null
	public isConnected = false

	constructor(
		private config: ProducerConfig,
		private mongoUri: string,
		private database: string,
	) {}

	async connect(): Promise<void> {
		this.client = new MongoClient(this.mongoUri)
		await this.client.connect()
		this.isConnected = true
		Logger.info(`Producer connected to MongoDB for topic ${this.config.topic}`)
	}

	// Método send com tipo genérico para melhor type safety
	async send<T = Document>(messages: Message<T> | Message<T>[]): Promise<void> {
		if (!this.isConnected) {
			throw new Error('Producer not connected')
		}

		if (!this.client) {
			throw new Error('MongoClient is not initialized')
		}

		const messageArray = Array.isArray(messages) ? messages : [messages]
		const db = this.client.db(this.database)
		const collection = db.collection(this.config.topic)

		const documents: OptionalId<Document>[] = messageArray.map((msg) => {
			const document: Document = {
				// Spread do value (que é do tipo T)
				...(msg.value as Document),
				// Adicionar metadados garantindo que não sejam undefined
				_headers: msg.headers || {},
				_timestamp: msg.timestamp || new Date(),
				_key: msg.key || null,
			}

			Object.keys(document).forEach((key) => {
				if (document[key] === undefined) {
					delete document[key]
				}
			})

			return document
		})

		try {
			if (documents.length === 1) {
				if (!documents[0]) {
					Logger.warn('No valid documents to send')
					return
				}
				await collection.insertOne(documents[0])
			} else {
				await collection.insertMany(documents)
			}
		} catch (error) {
			Logger.error(
				`Failed to send messages to topic ${this.config.topic}:`,
				error,
			)
			throw error
		}
	}

	async disconnect(): Promise<void> {
		if (this.client) {
			await this.client.close()
			this.isConnected = false
			Logger.info('Producer disconnected')
		}
	}
}
