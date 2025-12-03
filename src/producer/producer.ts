import * as crypto from 'node:crypto'
import { Document, MongoClient, OptionalId } from 'mongodb'
import { Message, ProducerConfig } from '../types'
import { ILogger, Logger } from '../utils/logger'

export class ChangeStreamProducer {
	private client: MongoClient | null = null
	public isConnected = false
	private partitionCounter: number = 0
	private logger: ILogger

	constructor(
		private config: ProducerConfig,
		private mongoUri: string,
		private database: string,
	) {
		this.logger = Logger.withContext('Producer')
	}

	async connect(): Promise<void> {
		this.client = new MongoClient(this.mongoUri)
		await this.client.connect()
		this.isConnected = true
		this.logger.info(
			`Producer connected to MongoDB for topic ${this.config.topic}`,
		)
	}

	// Método send com tipo genérico para melhor type safety
	async send<T extends Document = Document>(
		messages: Message<T> | Message<T>[],
	): Promise<void> {
		if (!this.isConnected) {
			throw new Error('Producer not connected')
		}

		if (!this.client) {
			throw new Error('MongoClient is not initialized')
		}

		const messageArray = Array.isArray(messages) ? messages : [messages]
		const db = this.client.db(this.database)

		// Agrupar mensagens por partição
		const messagesByPartition = new Map<number, OptionalId<Document>[]>()

		for (const msg of messageArray) {
			const partition = this.getPartition(msg)

			if (!messagesByPartition.has(partition)) {
				messagesByPartition.set(partition, [])
			}

			const document: Document = {
				// Spread do value (que é do tipo T)
				...(msg.value as Document),
				// Adicionar metadados garantindo que não sejam undefined
				_headers: msg.headers || {},
				_timestamp: msg.timestamp || new Date(),
				_key: msg.key || null,
				_partition: partition,
			}

			Object.keys(document).forEach((key) => {
				if (document[key] === undefined) {
					delete document[key]
				}
			})

			const getPartitionToAddMessage = messagesByPartition.get(partition)

			if (!getPartitionToAddMessage) {
				throw new Error('Unexpected error: partition not found in map')
			}
			getPartitionToAddMessage.push(document)
		}

		// Inserir em cada partição
		for (const [partition, documents] of messagesByPartition) {
			const collectionName = this.getPartitionCollectionName(partition)
			const collection = db.collection(collectionName)

			try {
				if (documents.length === 1) {
					if (!documents[0]) {
						throw new Error('Document is undefined')
					}
					await collection.insertOne(documents[0])
				} else {
					await collection.insertMany(documents)
				}

				this.logger.info(
					`Sent ${documents.length} messages to partition ${partition}`,
				)
			} catch (error) {
				this.logger.error(
					`Failed to send messages to partition ${partition}:`,
					error,
				)
				throw error
			}
		}
	}

	private getPartition(message: Message): number {
		const strategy = this.config.partitionStrategy || 'hash'

		switch (strategy) {
			case 'hash':
				return this.getHashPartition(message)
			case 'round-robin':
				return this.getRoundRobinPartition()
			case 'key-based':
				return this.getKeyBasedPartition(message)
			default:
				return this.getHashPartition(message)
		}
	}

	private getHashPartition(message: Message): number {
		// Usar a key se disponível, senão usar conteúdo da mensagem
		const partitionKey = message.key || JSON.stringify(message.value)

		const hash = crypto.createHash('md5').update(partitionKey).digest('hex')
		const numericHash = parseInt(hash.substring(0, 8), 16)

		return Math.abs(numericHash) % this.config.partitions
	}

	private getRoundRobinPartition(): number {
		const partition = this.partitionCounter % this.config.partitions
		this.partitionCounter = (this.partitionCounter + 1) % this.config.partitions
		return partition
	}

	private getKeyBasedPartition(message: Message): number {
		if (!message.key) {
			return this.getHashPartition(message)
		}

		// Hash simples baseado apenas na key
		const hash = crypto.createHash('md5').update(message.key).digest('hex')
		const numericHash = parseInt(hash.substring(0, 8), 16)

		return Math.abs(numericHash) % this.config.partitions
	}

	private getPartitionCollectionName(partition: number): string {
		return `${this.config.topic}_p${partition}`
	}

	async disconnect(): Promise<void> {
		if (this.client) {
			await this.client.close()
			this.isConnected = false
			this.logger.info('Producer disconnected')
		}
	}
}
