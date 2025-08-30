import {
	CollationOptions,
	Document,
	ChangeStreamOptions as MongoChangeStreamOptions,
	ObjectId,
	OptionalUnlessRequiredId,
	ReadPreferenceLike,
	ResumeToken,
	Timestamp,
	WithId,
} from 'mongodb'

export interface BrokerConfig {
	mongoUri: string
	database?: string
	appName?: string
	maxRetries?: number
	retryDelayMs?: number
	heartbeatIntervalMs?: number
	autoCreateTopics?: boolean
}

export interface TopicConfig {
	name: string
	collection: string
	partitions: number
	retentionMs: number
}

export interface TopicDocument extends Document, TopicConfig {
	_id?: ObjectId
	name: string
	collection: string
	partitions: number
	retentionMs: number
	createdAt: Date
	updatedAt: Date
}

// Tipo para documentos com _id
export type TopicDocumentWithId = WithId<TopicDocument>

// Tipo para operações de upsert
export type TopicDocumentInput = OptionalUnlessRequiredId<TopicDocument>

export interface TTLIndexInfo {
	v: number
	key: { [key: string]: number }
	name: string
	ns?: string
	expireAfterSeconds: number
	background?: boolean
	unique?: boolean
	sparse?: boolean
}

export interface TTLValidationResult {
	isValid: boolean
	issues: string[]
	indexInfo?: TTLIndexInfo | undefined
	sampleDocument?: any
}

export interface IndexOperationResult {
	success: boolean
	message: string
	indexInfo?: TTLIndexInfo | undefined
}

export interface ConsumerConfig {
	groupId: string
	topic: string
	autoCommit?: boolean
	autoCommitIntervalMs?: number
	fromBeginning?: boolean
	maxRetries: number
	retryDelayMs: number
	options?: ChangeStreamWatchOptions
}

export interface ProducerConfig {
	topic: string
	partitions: number
	retentionMs: number
}

export interface Message<T = Document> {
	key?: string
	value: T
	timestamp: Date
	headers?: Record<string, string>
}

export interface ConsumerRecord<T extends Document = Document> {
	topic: string
	partition: number
	message: Message<T>
	offset: ResumeToken // Resume token como offset
	timestamp: Date
}

export interface OffsetStorage {
	connect(): Promise<void>
	disconnect(): Promise<void>
	commitOffset(commit: OffsetCommit): Promise<void>
	getOffset(
		groupId: string,
		topic: string,
		partition: number,
	): Promise<ResumeToken | null>
	getOffsets(groupId: string): Promise<OffsetCommit[]>
	isConnected?(): boolean
	getConnectionInfo?(): { database: string; collection: string }
}

export interface OffsetCommit {
	topic: string
	partition: number
	groupId: string
	offset: ResumeToken
	timestamp: Date
}

export interface ConsumerGroup {
	groupId: string
	topics: string[]
	lastOffsets: Map<string, ResumeToken> // Mapa de tópico para último offset (resume token)
	members: ConsumerMap
}

interface ChangeStreamConsumer {
	// Identificação do consumer
	getConsumerId(): string
	getGroupId(): string
	getTopic(): string

	// Gerenciamento de conexão
	connect(): Promise<void>
	disconnect(): Promise<void>

	// Assinatura e processamento
	subscribe<T extends Document = Document>(
		config: MessageHandlerConfig<T>,
	): Promise<void>
	unsubscribe(): Promise<void>

	// Gerenciamento de offsets
	commitOffsets(): Promise<void>
	getLastProcessedOffset(): ResumeToken | null

	// Status e controle
	isConnected(): boolean
	isSubscribed(): boolean
	pause(): void
	resume(): void
}

export type ConsumerMap = Map<string, ChangeStreamConsumer>

export interface ChangeStreamEvent<T extends Document = Document> {
	_id: ResumeToken
	operationType:
		| 'insert'
		| 'update'
		| 'replace'
		| 'delete'
		| 'invalidate'
		| 'drop'
		| 'rename'
		| 'shardCollection'
	fullDocument?: T
	ns?: {
		db: string
		coll: string
	}
	documentKey: { _id: ObjectId | unknown }
	updateDescription?: {
		// ← Disponível apenas para operationType: 'update'
		updatedFields: Partial<T>
		removedFields: (keyof T)[]
	}
	clusterTime: Date

	// Campos específicos para determinados operationTypes
	to?: {
		// ← Para operationType: 'rename'
		db: string
		coll: string
	}
}

// Interface auxiliar para eventos de atualização
export interface ChangeStreamUpdateEvent<T extends Document = Document>
	extends ChangeStreamEvent<T> {
	operationType: 'update'
	updateDescription: {
		updatedFields: Partial<T>
		removedFields: (keyof T)[]
	}
	fullDocument?: T
}

// Interface auxiliar para eventos de insert
export interface ChangeStreamInsertEvent<T extends Document = Document>
	extends ChangeStreamEvent<T> {
	operationType: 'insert'
	fullDocument: T
}

// Interface auxiliar para eventos de delete
export interface ChangeStreamDeleteEvent<T extends Document = Document>
	extends ChangeStreamEvent<T> {
	operationType: 'delete'
	documentKey: {
		_id: ObjectId | unknown
	}
}

// Interface auxiliar para eventos de replace
export interface ChangeStreamReplaceEvent<T extends Document = Document>
	extends ChangeStreamEvent<T> {
	operationType: 'replace'
	fullDocument: T
}

export interface MessageHandler<T = Document> {
	/**
	 * Processa uma mensagem recebida do Change Stream
	 * @param record O registro do consumer contendo a mensagem e metadados
	 * @returns Promise<void>
	 */
	(record: ConsumerRecord & { message: { value: T } }): Promise<void>
}

export interface ErrorHandler {
	/**
	 * Trata erros ocorridos durante o processamento de mensagens
	 * @param error O erro ocorrido
	 * @param record O registro que estava sendo processado (pode ser null se não houver documento)
	 * @returns Promise<void>
	 */
	(error: Error, record?: ConsumerRecord | null): Promise<void>
}

export interface MessageHandlerConfig<T = Document> {
	/**
	 * Handler principal para processamento de mensagens
	 */
	handler: MessageHandler<T>

	/**
	 * Handler para tratamento de erros (opcional)
	 */
	errorHandler?: ErrorHandler | undefined

	/**
	 * Número máximo de tentativas de reprocessamento
	 * @default 3
	 */
	maxRetries?: number | undefined

	/**
	 * Tempo de espera entre tentativas (em ms)
	 * @default 1000
	 */
	retryDelay?: number | undefined

	/**
	 * Se deve comitar automaticamente após processamento bem-sucedido
	 * @default true
	 */
	autoCommit?: boolean | undefined
}

export interface ChangeStreamWatchOptions extends MongoChangeStreamOptions {
	fullDocument?: 'default' | 'updateLookup' | 'whenAvailable' | 'required'
	resumeAfter?: ResumeToken
	startAfter?: ResumeToken
	startAtOperationTime?: Timestamp
	batchSize?: number
	maxAwaitTimeMS?: number
	collation?: CollationOptions
	readPreference?: ReadPreferenceLike
}
