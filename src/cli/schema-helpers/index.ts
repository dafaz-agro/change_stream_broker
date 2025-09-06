import {
	BrokerConfig,
	ConsumerConfig,
	ProducerConfig,
	TopicConfig,
} from '../../types'

export const defineBroker = (config: BrokerConfig): BrokerConfig => config
export const defineTopic = (config: TopicConfig): TopicConfig => config
export const defineConsumer = (config: ConsumerConfig): ConsumerConfig => config
export const defineProducer = (config: ProducerConfig): ProducerConfig => config
