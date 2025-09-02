import { Logger } from '@/utils/logger'
import { ChangeStreamConsumer } from '../consumer/consumer'
import { ConsumerGroupManager } from '../consumer/consumer-group'
import { ChangeStreamProducer } from '../producer/producer'
import { OffsetStorage } from '../storage/offset-storage'
import {
	BrokerConfig,
	ConsumerConfig,
	LogLevel,
	ProducerConfig,
	TopicConfig,
} from '../types'
import { TopicManager } from './topic-manager'

export class ChangeStreamBroker {
	private topicManager: TopicManager
	private offsetStorage: OffsetStorage
	private consumerGroupManager: ConsumerGroupManager
	private consumers: Map<string, ChangeStreamConsumer> = new Map()
	private producers: Map<string, ChangeStreamProducer> = new Map()

	constructor(private config: BrokerConfig) {
		this.configureGlobalLogger()

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

	private configureGlobalLogger(): void {
		const logLevel = this.convertLogLevel(this.config.logLevel)
		const logContext = this.config.logContext || 'ChangeStreamBroker'

		// Configurar Logger global
		Logger.configure({
			level: logLevel,
			enabled: logLevel > LogLevel.SILENT, // Desativar se for SILENT
			context: logContext,
		})

		// Log inicial se não for SILENT
		if (logLevel > LogLevel.SILENT) {
			Logger.info('Broker initialized', {
				logLevel: LogLevel[logLevel],
				context: logContext,
			})
		}
	}

	private convertLogLevel(levelStr?: string): LogLevel {
		if (!levelStr) return LogLevel.SILENT // Default: SILENT

		const upperLevel = levelStr.toUpperCase()
		switch (upperLevel) {
			case 'SILENT':
				return LogLevel.SILENT
			case 'ERROR':
				return LogLevel.ERROR
			case 'WARN':
				return LogLevel.WARN
			case 'INFO':
				return LogLevel.INFO
			case 'DEBUG':
				return LogLevel.DEBUG
			default:
				console.warn(`Unknown log level: ${levelStr}. Using SILENT.`)
				return LogLevel.SILENT
		}
	}

	async connect(): Promise<void> {
		await this.topicManager.connect()
		await this.offsetStorage.connect()
	}

	async createTopic(config: TopicConfig): Promise<void> {
		await this.topicManager.createTopic(config)
	}

	async createConsumer(config: ConsumerConfig): Promise<ChangeStreamConsumer> {
		const consumer = new ChangeStreamConsumer(
			config,
			this.offsetStorage,
			this.consumerGroupManager,
			this.config.mongoUri,
			this.config.database || 'change-stream-broker',
		)

		await consumer.connect()

		// Chave única por grupo + tópico + partições
		const consumerKey = `${config.groupId}-${config.topic}-${config.partitions.join(',')}`
		this.consumers.set(consumerKey, consumer)

		return consumer
	}

	async createProducer(config: ProducerConfig): Promise<ChangeStreamProducer> {
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
