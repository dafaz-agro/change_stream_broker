import path from 'node:path'
import { format } from 'date-fns'
import dotenv from 'dotenv'
import fs from 'fs-extra'
import { MongoClient } from 'mongodb'
import ts from 'typescript'
import {
	BrokerConfig,
	ConsumerConfig,
	ProducerConfig,
	TopicConfig,
} from '../../types'

interface SchemaAnalysis {
	interfaces: Array<{ name: string; content: string }>
	topicMappings: Array<{ topic: string; payloadType: string }>
}

interface ConfigAnalysis {
	brokerConfig: BrokerConfig
	topics: Array<{ name: string; config: TopicConfig }>
	producers: Array<{ name: string; config: ProducerConfig }>
	consumers: Array<{ name: string; config: ConsumerConfig }>
}

async function generateIndexFile(outputDir: string): Promise<void> {
	const indexContent = `// Auto-generated index file
export { broker } from './broker.client'
export * from './broker.client'

// Utility function to list available backups
export async function listClientBackups(): Promise<string[]> {
  const fs = require('fs').promises
  const path = require('path')
  
  try {
    const files = await fs.readdir(__dirname)
    return files
      .filter(file => file.startsWith('broker.client.ts_') && !file.endsWith('.map'))
      .sort()
      .reverse() // Most recent first
  } catch (error) {
    return []
  }
}

// Utility function to get backup content
export async function getBackupContent(backupName: string): Promise<string> {
  const fs = require('fs').promises
  const path = require('path')
  
  try {
    const content = await fs.readFile(path.join(__dirname, backupName), 'utf-8')
    return content
  } catch (error) {
    throw new Error(\`Backup \${backupName} not found\`)
  }
}
`
	await fs.writeFile(path.join(outputDir, 'index.ts'), indexContent)
}

export async function getPackageDir(): Promise<string> {
	try {
		// Tentativa 1: Usando require.resolve (production)
		return path.dirname(require.resolve('@dafaz/change-stream-broker'))
	} catch {
		// Tentativa 2: Procurar manualmente no node_modules (dev com link)
		const possiblePaths = [
			path.join(
				process.cwd(),
				'node_modules',
				'@dafaz',
				'change-stream-broker',
			),
			path.join(
				process.cwd(),
				'..',
				'node_modules',
				'@dafaz',
				'change-stream-broker',
			),
			path.join(
				process.cwd(),
				'..',
				'..',
				'node_modules',
				'@dafaz',
				'change-stream-broker',
			),
		]

		for (const possiblePath of possiblePaths) {
			if (await fs.pathExists(possiblePath)) {
				return possiblePath
			}
		}

		// Tentativa 3: Se estiver em monorepo ou estrutura alternativa
		const nodeModulesPath = path.join(process.cwd(), 'node_modules')
		const packages = await fs.readdir(nodeModulesPath).catch(() => [])

		for (const pkg of packages) {
			if (pkg.startsWith('@dafaz')) {
				const daFazPath = path.join(nodeModulesPath, pkg)
				const brokerPath = path.join(daFazPath, 'change-stream-broker')
				if (await fs.pathExists(brokerPath)) {
					return brokerPath
				}
			}
		}

		throw new Error(
			'Could not find @dafaz/change-stream-broker package. ' +
				'Make sure it is installed or properly linked with npm link.',
		)
	}
}

export function generateTimestamp(): string {
	return format(new Date(), 'yyyyMMdd_HHmmss')
}

async function backupExistingClient(outputPath: string): Promise<void> {
	if (await fs.pathExists(outputPath)) {
		const timestamp = generateTimestamp()
		const backupPath = `${outputPath}_${timestamp}`

		await fs.copy(outputPath, backupPath)
		console.log(`📦 Backup created: ${path.basename(backupPath)}`)
	}
}

// Função para listar backups antigos (opcional: limpeza)
async function listBackups(outputDir: string): Promise<string[]> {
	try {
		const files = await fs.readdir(outputDir)
		return files
			.filter(
				(file) =>
					file.startsWith('broker.client.ts_') && file !== 'broker.client.ts',
			)
			.sort()
	} catch {
		return []
	}
}

async function cleanupOldBackups(
	outputDir: string,
	maxBackups: number = 10,
): Promise<void> {
	try {
		const backups = await listBackups(outputDir)

		if (backups.length > maxBackups) {
			const backupsToDelete = backups.slice(0, backups.length - maxBackups)

			for (const backup of backupsToDelete) {
				await fs.remove(path.join(outputDir, backup))
				console.log(`🗑️  Deleted old backup: ${backup}`)
			}
		}
	} catch (error) {
		console.warn('Could not cleanup old backups:', error)
	}
}

export async function generateClient(): Promise<void> {
	try {
		validateEnvVariables()
		// await validateMongoDBConnection()

		const configDir = path.join(process.cwd(), 'change-stream')
		const configPath = path.join(configDir, 'config.ts')
		const schemaPath = path.join(configDir, 'message-payload.schema.ts')

		const packageDir = await getPackageDir()

		const outputDir = path.join(packageDir, 'client')
		const outputPath = path.join(outputDir, 'broker.client.ts')

		// Verificar se os arquivos existem
		if (!(await fs.pathExists(configPath))) {
			throw new Error('config.ts not found. Run "csbroker init" first.')
		}
		if (!(await fs.pathExists(schemaPath))) {
			throw new Error(
				'message-payload.schema.ts not found. Run "csbroker init" first.',
			)
		}

		// 1. Fazer backup do arquivo atual se existir
		await backupExistingClient(outputPath)

		// Analisar o schema para extrair interfaces e mapeamentos
		const schemaContent = await fs.readFile(schemaPath, 'utf-8')
		const schemaAnalysis = analyzeSchema(schemaContent)

		// Analisar o config para extrair tópicos configurados
		const configContent = await fs.readFile(configPath, 'utf-8')
		const configAnalysis = analyzeConfig(configContent)

		// Gerar cliente genérico
		const clientContent = generateGenericClient(schemaAnalysis, configAnalysis)

		await fs.ensureDir(outputDir)
		await fs.writeFile(outputPath, clientContent)

		console.log('✅ Client generated successfully!')
		console.log(
			'📁 File created: node_modules/@dafaz/change-stream-broker/client/broker.client.ts',
		)

		await generateIndexFile(outputDir)

		await cleanupOldBackups(outputDir, 10) // Manter últimos 10 backups
	} catch (error) {
		console.error('❌ Failed to generate client:')
		if (error instanceof Error) {
			if (error.message.includes('Missing required environment variables')) {
				console.log(
					'\n💡 Dica: Crie um arquivo .env na raiz do seu projeto com:',
				)
				console.log(
					'MONGODB_BROKER_URI=mongodb://usuario:senha@localhost:27017?replicaSet=rs0&authSource=admin',
				)
				console.log('MONGODB_BROKER_DATABASE=nome-do-banco')
			} else if (error.message.includes('replicaSet')) {
				console.log('\n❗ Erro de configuração do MongoDB:')
				console.log(
					'\n Se estiver usando um container docker, verifique a configuração correta do docker-compose.yml',
				)
				console.log(
					'\n ao final, em: https://www.npmjs.com/package/@dafaz/change_stream_broker',
				)
				console.log('\n Verifique se sua URI de conexxão está correta')
				console.log(
					'\n Ela deve conter algo como mongodb://[usuário]:[senha]@127.0.0.1:27017/?replicaSet=rs0&authSource=admin',
				)
				console.log(
					'\n Ou seja, deve conter o valor do query param de replicação: ?replicaSet=rs0.',
				)
			} else {
				console.error(error.message)
			}
		}
		process.exit(1)
	}
}

function analyzeSchema(content: string): SchemaAnalysis {
	const sourceFile = ts.createSourceFile(
		'schema.ts',
		content,
		ts.ScriptTarget.Latest,
		true,
	)

	const interfaces: Array<{ name: string; content: string }> = []
	const topicMappings: Array<{ topic: string; payloadType: string }> = []

	function visit(node: ts.Node) {
		// Coletar interfaces com seu conteúdo completo
		if (ts.isInterfaceDeclaration(node) && node.name) {
			// Pular a interface MessagePayloads - ela será gerada separadamente
			if (node.name.text !== 'MessagePayloads') {
				const interfaceContent = node.getText()
				interfaces.push({
					name: node.name.text,
					content: interfaceContent,
				})
			}
		}

		// Coletar mapeamentos de tópicos do MessagePayloads
		if (
			ts.isInterfaceDeclaration(node) &&
			node.name?.text === 'MessagePayloads'
		) {
			node.members.forEach((member) => {
				if (ts.isPropertySignature(member) && member.name && member.type) {
					const topic = member.name.getText().replace(/'/g, '')
					const payloadType = member.type.getText()
					topicMappings.push({ topic, payloadType })
				}
			})
		}

		ts.forEachChild(node, visit)
	}

	visit(sourceFile)
	return { interfaces, topicMappings }
}

function analyzeConfig(content: string): ConfigAnalysis {
	const sourceFile = ts.createSourceFile(
		'config.ts',
		content,
		ts.ScriptTarget.Latest,
		true,
	)

	const brokerConfig: Partial<BrokerConfig> = {}
	const topics: Array<{ name: string; config: TopicConfig }> = []
	const producers: Array<{ name: string; config: ProducerConfig }> = []
	const consumers: Array<{ name: string; config: ConsumerConfig }> = []

	function visit(node: ts.Node) {
		// Extrair configuração do broker
		if (
			ts.isVariableDeclaration(node) &&
			node.name.getText() === 'brokerConfig' &&
			node.initializer &&
			ts.isCallExpression(node.initializer) &&
			node.initializer.expression.getText() === 'defineBroker'
		) {
			extractObjectProperties(node.initializer.arguments[0], brokerConfig)
		}

		// Extrair tópicos, producers e consumers
		if (ts.isVariableStatement(node)) {
			node.declarationList.declarations.forEach((decl) => {
				if (
					ts.isIdentifier(decl.name) &&
					decl.initializer &&
					ts.isCallExpression(decl.initializer)
				) {
					const varName = decl.name.text
					const funcName = decl.initializer.expression.getText()
					const config: any = {}

					extractObjectProperties(decl.initializer.arguments[0], config)

					if (funcName === 'defineTopic') {
						topics.push({ name: varName, config })
					} else if (funcName === 'defineProducer') {
						producers.push({ name: varName, config })
					} else if (funcName === 'defineConsumer') {
						if (config.partitions && typeof config.partitions === 'string') {
							try {
								config.partitions = JSON.parse(config.partitions)
							} catch {
								// Se não for JSON válido, mantém como está
							}
						}
						consumers.push({ name: varName, config })
					}
				}
			})
		}

		ts.forEachChild(node, visit)
	}

	function extractObjectProperties(
		node: ts.Node | undefined,
		target: Record<string, any>,
	) {
		if (node && ts.isObjectLiteralExpression(node)) {
			node.properties.forEach((prop) => {
				if (ts.isPropertyAssignment(prop) && ts.isIdentifier(prop.name)) {
					target[prop.name.text] = extractValue(prop.initializer)
				}
			})
		}
	}

	function extractValue(node: ts.Node): any {
		if (ts.isStringLiteral(node)) return node.text
		if (ts.isNumericLiteral(node)) return Number(node.text)
		if (node.kind === ts.SyntaxKind.TrueKeyword) return true
		if (node.kind === ts.SyntaxKind.FalseKeyword) return false
		if (ts.isIdentifier(node)) return node.getText()
		if (ts.isPropertyAccessExpression(node)) return node.getText()

		// Tenta calcular expressões binárias (matemáticas)
		if (ts.isBinaryExpression(node)) {
			try {
				const left = extractValue(node.left)
				const right = extractValue(node.right)

				if (typeof left === 'number' && typeof right === 'number') {
					switch (node.operatorToken.kind) {
						case ts.SyntaxKind.AsteriskToken:
							return left * right
						case ts.SyntaxKind.PlusToken:
							return left + right
						case ts.SyntaxKind.MinusToken:
							return left - right
						case ts.SyntaxKind.SlashToken:
							return left / right
					}
				}
			} catch {
				// Se não conseguir calcular, retorna a expressão como string
			}
		}

		if (ts.isArrayLiteralExpression(node)) {
			return node.elements.map((element) => extractValue(element))
		}

		if (ts.isObjectLiteralExpression(node)) {
			const obj: Record<string, any> = {}
			node.properties.forEach((prop) => {
				if (ts.isPropertyAssignment(prop) && ts.isIdentifier(prop.name)) {
					obj[prop.name.text] = extractValue(prop.initializer)
				}
			})
			return obj
		}

		// Para expressões complexas, retorna como string para ser avaliada em runtime
		return node.getText()
	}

	visit(sourceFile)

	// Completar a configuração com valores padrão
	const completeConfig: BrokerConfig = {
		mongoUri: brokerConfig.mongoUri || process.env.MONGODB_BROKER_URI || '',
		database:
			brokerConfig.database || process.env.MONGODB_BROKER_DATABASE || '',
		autoCreateTopics: brokerConfig.autoCreateTopics ?? true,
		logLevel: brokerConfig.logLevel || 'INFO',
		logContext: brokerConfig.logContext || 'ChangeStreamBroker',
		...brokerConfig, // spread para manter outras propriedades
	}

	return { brokerConfig: completeConfig, topics, producers, consumers }
}

function generateGenericClient(
	schema: SchemaAnalysis,
	config: ConfigAnalysis,
): string {
	const capitalize = (str: string): string => {
		return str.charAt(0).toUpperCase() + str.slice(1)
	}

	function formatValue(value: any): string {
		if (typeof value === 'string') {
			if (value.includes('process.env.')) {
				return value
			}
			return `'${value}'`
		}
		if (typeof value === 'number') return value.toString()
		if (typeof value === 'boolean') return value.toString()
		if (Array.isArray(value)) {
			return `[${value.map((v) => formatValue(v)).join(', ')}]`
		}
		if (typeof value === 'object' && value !== null) {
			// Verificação profunda para objetos - só inclui propriedades definidas
			const entries = Object.entries(value)
				.filter(([_, val]) => val !== undefined && val !== null) // Filtra undefined/null
				.map(([key, val]) => `${key}: ${formatValue(val)}`)

			if (entries.length === 0) return 'undefined'
			return `{ ${entries.join(', ')} }`
		}
		return String(value)
	}

	// Seção de configurações
	const configSection = `
// ==============================================
// CONFIGURATIONS (from config.ts)
// ==============================================

// Broker Configuration

if (!process.env.MONGODB_BROKER_URI) {
	throw new Error('Environment variable MONGODB_BROKER_URI is not set.')
}

if (!process.env.MONGODB_BROKER_DATABASE) {
	throw new Error('Environment variable MONGODB_BROKER_URI is not set.')
}

export const brokerConfig: BrokerConfig = {
${Object.entries(config.brokerConfig)
	.map(([key, value]) => `  ${key}: ${formatValue(value)}`)
	.join(',\n')}
};

// Producers Configuration
${config.producers
	.map(
		(producer) => `export const ${producer.name}: ProducerConfig = {
${Object.entries(producer.config)
	.map(([key, value]) => `  ${key}: ${formatValue(value)}`)
	.join(',\n')}
};
`,
	)
	.join('')}

// Consumers Configuration
${config.consumers
	.map(
		(consumer) => `export const ${consumer.name}: ConsumerConfig = {
${Object.entries(consumer.config)
	.filter(([_key, value]) => value !== undefined && value !== null)
	.map(([key, value]) => `  ${key}: ${formatValue(value)}`)
	.join(',\n')}
};
`,
	)
	.join('')}
`

	// Seção de métodos para Producers
	const producerHelpersSection =
		config.producers.length > 0
			? `
// ==============================================
// PRODUCER METHODS
// ==============================================
${config.producers
	.map((producer) => {
		const mapping = schema.topicMappings.find(
			(m) => m.topic === producer.config.topic,
		)
		const payloadType = mapping ? mapping.payloadType : 'any'
		const producerName = capitalize(producer.name)

		return `
/**
 * Send message to ${producer.config.topic}
 */
export async function sendTo${producerName}(
  message: ${payloadType},
  options?: {
    key?: string;
    timestamp?: Date;
    headers?: Header;
  }
): Promise<void> {
  const producer = await broker.createProducer(${producer.name});

  let messageToSend!: Message

  if (options?.key !== undefined) messageToSend.key = options?.key;
  if (options?.timestamp !== undefined) messageToSend.timestamp = options.timestamp;

  if (options?.headers !== undefined && 
      (options.headers?.eventType !== undefined || options.headers?.source !== undefined)) {
        
      messageToSend.headers = {};
    
    if (options.headers?.eventType !== undefined) {
      messageToSend.headers.eventType = options.headers?.eventType;
    }
    
    if (options.headers?.source !== undefined) {
      messageToSend.headers.source = options.headers.source;
    }
  }

  messageToSend.value = message;

  await producer.send(messageToSend);
}

/**
 * Get ${producer.config.topic} producer instance
 */
export async function get${producerName}(): Promise<ChangeStreamProducer> {
  return await broker.createProducer(${producer.name});
}
`
	})
	.join('')}
`
			: ''

	// Seção de métodos para Consumers
	const consumerHelpersSection =
		config.consumers.length > 0
			? `
// ==============================================
// CONSUMER METHODS (with auto-binding)
// ==============================================
${config.consumers
	.map((consumer) => {
		const mapping = schema.topicMappings.find(
			(m) => m.topic === consumer.config.topic,
		)
		const payloadType = mapping ? mapping.payloadType : 'any'
		const consumerName = capitalize(consumer.name)

		return `
/**
 * Subscribe to ${consumer.config.topic} with automatic context binding
 */
export async function subscribeTo${consumerName}(
  context: any,
  handlers: {
    handler: (record: ConsumerRecord<${payloadType}>) => Promise<void>;
    errorHandler?: (error: Error, record?: ConsumerRecord<${payloadType}>) => Promise<void>;
  }
): Promise<ChangeStreamConsumer> {
  const consumer = await broker.createConsumer(${consumer.name});

  const boundConfig: MessageHandlerConfig<${payloadType}> = {
    handler: handlers.handler.bind(context),
    errorHandler: handlers.errorHandler?.bind(context),
    maxRetries: ${consumer.config.maxRetries || 3},
    retryDelay: ${consumer.config.retryDelayMs || 1000},
    autoCommit: ${consumer.config.autoCommit ?? true}
  };

  await consumer.subscribe(boundConfig);
  return consumer;
}

/**
 * Get ${consumer.config.topic} consumer instance
 */
export async function get${consumerName}(): Promise<ChangeStreamConsumer> {
  return await broker.createConsumer(${consumer.name});
}
`
	})
	.join('')}
`
			: ''

	// Métodos genéricos
	const genericHelpersSection = `
// ==============================================
// GENERIC METHODS
// ==============================================

/**
 * Initialize broker and create all configured topics
 */
export async function initializeBroker(): Promise<void> {
  await broker.connect();
  ${config.topics.map((topic) => `await broker.createTopic(${topic.name});`).join('\n  ')}
}

/**
 * Disconnect broker
 */
export async function disconnectBroker(): Promise<void> {
  await broker.disconnect();
}

// Generic producer method
${
	config.producers.length > 0
		? `
export async function sendMessage<T extends MessageType>(
  topic: T,
  message: MessagePayloads[T],
  options?: {
    key?: string;
    timestamp?: Date;
    headers?: Header;
  }
): Promise<void> {
  const producerConfig = getProducerConfig(topic.toString());
  const producer = await broker.createProducer(producerConfig);

 let messageToSend!: Message

  if (options?.key !== undefined) messageToSend.key = options?.key;
  if (options?.timestamp !== undefined) messageToSend.timestamp = options.timestamp;

  if (options?.headers !== undefined && 
      (options.headers?.eventType !== undefined || options.headers?.source !== undefined)) {
        
      messageToSend.headers = {};
    
    if (options.headers?.eventType !== undefined) {
      messageToSend.headers.eventType = options.headers?.eventType;
    }
    
    if (options.headers?.source !== undefined) {
      messageToSend.headers.source = options.headers.source;
    }
  }

  messageToSend.value = message;

  await producer.send(messageToSend);
}
`
		: ''
}

// Generic consumer method
${
	config.consumers.length > 0
		? `
export async function subscribeToTopic<T extends MessageType>(
  topic: T,
  groupId: string,
  context: any,
  handlers: {
    handler: (record: ConsumerRecord<MessagePayloads[T]>) => Promise<void>;
    errorHandler?: (error: Error, record?: ConsumerRecord<MessagePayloads[T]>) => Promise<void>;
  }
): Promise<ChangeStreamConsumer> {
  const consumerConfig = getConsumerConfig(groupId, topic.toString());
  const consumer = await broker.createConsumer(consumerConfig);

  const boundConfig: MessageHandlerConfig<MessagePayloads[T]> = {
    handler: handlers.handler.bind(context),
    errorHandler: handlers.errorHandler?.bind(context),
    maxRetries: consumerConfig.maxRetries || 3,
    retryDelay: consumerConfig.retryDelayMs || 1000,
    autoCommit: consumerConfig.autoCommit ?? true
  };

  await consumer.subscribe(boundConfig);
  return consumer;
}
`
		: ''
}

// Configuration helpers
${
	config.producers.length > 0
		? `
function getProducerConfig(topic: string): ProducerConfig {
  ${config.producers
		.map(
			(producer) => `
  if (topic === '${producer.config.topic}') {
    return ${producer.name};
  }`,
		)
		.join('')}

  throw new Error(\`Producer not configured for topic: \${topic}\`);
}
`
		: ''
}

${
	config.consumers.length > 0
		? `
function getConsumerConfig(groupId: string, topic: string): ConsumerConfig {
  ${config.consumers
		.map(
			(consumer) => `
  if (groupId === '${consumer.config.groupId}' && topic === '${consumer.config.topic}') {
    return ${consumer.name};
  }`,
		)
		.join('')}

  throw new Error(\`Consumer not configured for group: \${groupId} and topic: \${topic}\`);
}
`
		: ''
}
`

	return `// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from change-stream/config.ts and change-stream/message-payload.schema.ts

import {
	BrokerConfig,
	ChangeStreamBroker,
	ChangeStreamConsumer,
	ChangeStreamProducer,
	ConsumerConfig,
	ConsumerRecord,
	Header,
	Message,
	MessageHandlerConfig,
	ProducerConfig,
} from '@dafaz/change-stream-broker'

// ==============================================
// MESSAGE PAYLOAD INTERFACES (from schema)
// ==============================================
${schema.interfaces.map((iface) => iface.content).join('\n\n')}

// ==============================================
// TOPIC TO PAYLOAD MAPPING
// ==============================================
export interface MessagePayloads {
${schema.topicMappings.map((mapping) => `  '${mapping.topic}': ${mapping.payloadType};`).join('\n')}
  [topic: string]: Record<string, any>;
}

${configSection}

// ==============================================
// BROKER INSTANCE
// ==============================================
export const broker = new ChangeStreamBroker(brokerConfig);

// ==============================================
// TYPE UTILITIES
// ==============================================
export type MessageType = keyof MessagePayloads;

${producerHelpersSection}
${consumerHelpersSection}
${genericHelpersSection}

// ==============================================
// DEFAULT EXPORT
// ==============================================
export default broker;
`
}

function getEnvVariables(): Record<string, string> {
	const envPath = path.join(process.cwd(), '.env')

	// Tentar ler do arquivo .env
	if (fs.existsSync(envPath)) {
		return dotenv.parse(fs.readFileSync(envPath))
	}

	// Fallback para process.env (caso estejam carregadas de outra forma)
	return process.env as Record<string, string>
}

function validateEnvVariables(): void {
	const envConfig = getEnvVariables()

	const requiredEnvVars = ['MONGODB_BROKER_URI', 'MONGODB_BROKER_DATABASE']
	const missingEnvVars = requiredEnvVars.filter(
		(envVar) => !envConfig[envVar] || envConfig[envVar].trim() === '',
	)

	if (missingEnvVars.length > 0) {
		throw new Error(
			`Missing required environment variables: ${missingEnvVars.join(', ')}\n\n` +
				'Please add them to your .env file in the project root:\n\n' +
				'MONGODB_BROKER_URI=mongodb://localhost:27017/your-database?replicaSet=rs0&authSource=admin\n' +
				'MONGODB_BROKER_DATABASE=your-database-name\n\n' +
				'💡 Example .env file:\n' +
				'MONGODB_BROKER_URI=mongodb://user:pass@localhost:27017,localhost:27018/db?replicaSet=rs0&authSource=admin\n' +
				'MONGODB_BROKER_DATABASE=my-app-db',
		)
	}

	// Validação adicional do formato da URI
	const uri = envConfig.MONGODB_BROKER_URI
	if (uri && !uri.includes('replicaSet=') && !uri.includes('mongodb+srv://')) {
		console.warn(
			'⚠️  Warning: MongoDB URI does not contain replicaSet parameter',
		)
		console.warn('   Change Streams require replicaSet configuration')
		console.warn('   Consider adding ?replicaSet=rs0 to your connection string')
	}
}

async function _validateMongoDBConnection(): Promise<void> {
	const uri = process.env.MONGODB_BROKER_URI

	if (!uri) {
		throw new Error('MONGODB_BROKER_URI is required for connection validation')
	}

	let client: MongoClient | null = null

	try {
		client = new MongoClient(uri, {
			connectTimeoutMS: 5000,
			serverSelectionTimeoutMS: 5000,
		})

		await client.connect()

		// Verificar se é um replicaSet
		const adminDb = client.db('admin')
		const replStatus = await adminDb
			.command({ replSetGetStatus: 1 })
			.catch(() => null)

		if (!replStatus) {
			throw new Error(
				'MongoDB instance is not configured as a replicaSet.\n' +
					'Change Streams require replicaSet configuration.\n\n' +
					'💡 Para configurar:\n' +
					'1. Adicione no mongod.conf: replication.replSetName = "rs0"\n' +
					'2. Reinicie o MongoDB\n' +
					'3. Execute: rs.initiate() no mongo shell\n\n' +
					'📖 Documentação: https://docs.mongodb.com/manual/changeStreams/',
			)
		}

		// Verificar se o replicaSet está healthy
		if (replStatus.ok === 1 && replStatus.members) {
			const healthyMembers = replStatus.members.filter(
				(member: any) => member.health === 1 && member.state === 1,
			)

			if (healthyMembers.length === 0) {
				throw new Error(
					'ReplicaSet has no healthy primary member.\n' +
						'Check your MongoDB replicaSet configuration.',
				)
			}
		}

		console.log('✅ MongoDB replicaSet validation passed')
	} catch (error) {
		if (error instanceof Error) {
			if (error.message.includes('replSetGetStatus')) {
				throw new Error(
					'MongoDB instance is not configured as a replicaSet.\n' +
						'Change Streams require replicaSet configuration.\n\n' +
						'Steps to fix:\n' +
						'1. Add to mongod.conf: replication.replSetName = "rs0"\n' +
						'2. Restart MongoDB\n' +
						'3. Run: rs.initiate() in mongo shell\n\n' +
						'Documentation: https://docs.mongodb.com/manual/changeStreams/',
				)
			}
			throw error
		}
		throw new Error(
			'Failed to validate MongoDB connection: '.concat(String(error)),
		)
	} finally {
		if (client) {
			await client.close()
		}
	}
}
