import { ChangeStreamConsumer } from '../consumer/consumer'
import { ConsumerGroupManager } from '../consumer/consumer-group'
import { ChangeStreamProducer } from '../producer/producer'
import { OffsetStorage } from '../storage/offset-storage'
import {
	BrokerConfig,
	ConsumerConfig,
	IChangeStreamConsumer,
	IChangeStreamProducer,
	ProducerConfig,
	TopicConfig,
} from '../types/types'
import { TopicManager } from './topic-manager'

export class ChangeStreamBroker {
	private topicManager: TopicManager
	private offsetStorage: OffsetStorage
	private consumerGroupManager: ConsumerGroupManager
	private consumers: Map<string, IChangeStreamConsumer> = new Map()
	private producers: Map<string, IChangeStreamProducer> = new Map()

	constructor(private config: BrokerConfig) {
		this.topicManager = new TopicManager(
			config.mongoUri,
			config.database || 'change-stream-broker',
		)

		this.offsetStorage = new OffsetStorage(
			config.mongoUri,
			config.database || 'change-stream-broker',
		)

		this.consumerGroupManager = new ConsumerGroupManager()
	}

	async connect(): Promise<void> {
		await this.topicManager.connect()
		await this.offsetStorage.connect()
	}

	async createTopic(config: TopicConfig): Promise<void> {
		await this.topicManager.createTopic(config)
	}

	async createConsumer(config: ConsumerConfig): Promise<IChangeStreamConsumer> {
		const consumer = new ChangeStreamConsumer(
			config,
			this.offsetStorage,
			this.consumerGroupManager,
			this.config.mongoUri,
			this.config.database || 'change-stream-broker',
		)

		await consumer.connect()
		this.consumers.set(`${config.groupId}-${config.topic}`, consumer)

		return consumer
	}

	async createProducer(config: ProducerConfig): Promise<IChangeStreamProducer> {
		// Verificar se o tópico existe, criar se necessário
		if (
			this.config.autoCreateTopics &&
			!(await this.topicManager.topicExists(config.topic))
		) {
			await this.createTopic({
				name: config.topic,
				collection: config.topic,
				partitions: config.partitions,
				retentionMs: config.retentionMs, //7 * 24 * 60 * 60 * 1000 => 7 dias
			})
		}

		const producer = new ChangeStreamProducer(
			config,
			this.config.mongoUri,
			this.config.database || 'change-stream-broker',
		)

		await producer.connect()
		this.producers.set(config.topic, producer)

		return producer
	}

	async disconnect(): Promise<void> {
		// Desconectar todos os consumers
		for (const [_, consumer] of this.consumers) {
			await consumer.disconnect()
		}
		this.consumers.clear()

		// Desconectar todos os producers
		for (const [_, producer] of this.producers) {
			await producer.disconnect()
		}
		this.producers.clear()

		await this.topicManager.disconnect()
		await this.offsetStorage.disconnect()
	}
}
