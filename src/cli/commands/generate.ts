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

interface ProjectConfig {
	compilerOptions: ts.CompilerOptions
	moduleSystem: 'esm' | 'cjs'
	projectRoot: string
}

function detectProjectConfig(
	projectRoot: string = process.cwd(),
): ProjectConfig {
	// Ler tsconfig
	const tsconfigPath = path.join(projectRoot, 'tsconfig.json')
	let compilerOptions: ts.CompilerOptions = {}

	if (fs.existsSync(tsconfigPath)) {
		try {
			const configFile = ts.readConfigFile(tsconfigPath, ts.sys.readFile)
			if (configFile.config) {
				const parsedConfig = ts.parseJsonConfigFileContent(
					configFile.config,
					ts.sys,
					path.dirname(tsconfigPath),
				)
				compilerOptions = parsedConfig.options
			}
		} catch {
			console.warn('⚠️  Could not parse tsconfig.json, using defaults')
		}
	}

	// Detectar sistema de módulos
	const packageJsonPath = path.join(projectRoot, 'package.json')
	let moduleSystem: 'esm' | 'cjs' = 'cjs'

	if (fs.existsSync(packageJsonPath)) {
		try {
			const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'))
			if (packageJson.type === 'module') {
				moduleSystem = 'esm'
			}
		} catch {
			console.warn('⚠️  Could not parse package.json, using CommonJS')
		}
	}

	// Configurações padrão com fallback inteligente
	return {
		compilerOptions: {
			target: compilerOptions.target || ts.ScriptTarget.ES2020,
			module:
				moduleSystem === 'esm' ? ts.ModuleKind.ESNext : ts.ModuleKind.CommonJS,
			esModuleInterop: compilerOptions.esModuleInterop ?? true,
			allowSyntheticDefaultImports:
				compilerOptions.allowSyntheticDefaultImports ?? true,
			strict: compilerOptions.strict ?? true,
			skipLibCheck: compilerOptions.skipLibCheck ?? true,
			lib: compilerOptions.lib || ['ES2020'],
			// Preservar outras opções relevantes
			...compilerOptions,
		},
		moduleSystem,
		projectRoot,
	}
}

function generateJavaScriptClient(
	tsContent: string,
	configAnalysis: ConfigAnalysis,
): string {
	const projectConfig = detectProjectConfig()

	// Configurações específicas para a geração do client
	const generationOptions: ts.CompilerOptions = {
		...projectConfig.compilerOptions,
		declaration: false,
		sourceMap: false,
		inlineSourceMap: false,
		outDir: '', // Não gerar arquivos de output
		rootDir: '',
	}

	const result = ts.transpileModule(tsContent, {
		compilerOptions: generationOptions,
	})

	let output = result.outputText

	if (projectConfig.moduleSystem === 'esm') {
		output = output
			// Remover "use strict" (não necessário em ESM)
			.replace(/^"use strict";\s*/gm, '')
			// Remover exports do CommonJS
			.replace(
				/Object\.defineProperty\(exports, "__esModule", \{ value: true \}\);\s*/g,
				'',
			)
			.replace(/exports\.\w+ = \w+;\s*/g, '')
			.replace(/exports\.default = \w+;\s*/g, '')
			// Converter module.exports para export default
			.replace(/module\.exports = (\w+);/g, 'export default $1;')
			// Converter exports.named para export named
			.replace(/exports\.(\w+) = (\w+);/g, 'export const $1 = $2;')
			// Remover require assignments
			.replace(
				/const (\w+) = require\(("[^"]+"|'[^']+')\);/g,
				'import $1 from $2;',
			)
			// Converter __exportStar para export *
			.replace(
				/__exportStar\(require\(("[^"]+"|'[^']+')\), exports\);/g,
				'export * from $1;',
			)
	}

	if (configAnalysis.topics.length > 0) {
		const topicsExport =
			projectConfig.moduleSystem === 'esm'
				? `export const topics = {${configAnalysis.topics.map((t) => `${t.name}: ${t.name}Config`).join(', ')}};`
				: `exports.topics = {${configAnalysis.topics.map((t) => `${t.name}: exports.${t.name}Config`).join(', ')}};`
		output += `\n\n// Topics namespace\n${topicsExport}`
	}

	if (configAnalysis.producers.length > 0) {
		const producersExport =
			projectConfig.moduleSystem === 'esm'
				? `export const producers = {${configAnalysis.producers.map((p) => `${p.name}: ${p.name}Config`).join(', ')}};`
				: `exports.producers = {${configAnalysis.producers.map((p) => `${p.name}: exports.${p.name}Config`).join(', ')}};`
		output += `\n\n// Producers namespace\n${producersExport}`
	}

	if (configAnalysis.consumers.length > 0) {
		const consumersExport =
			projectConfig.moduleSystem === 'esm'
				? `export const consumers = {${configAnalysis.consumers.map((c) => `${c.name}: ${c.name}Config`).join(', ')}};`
				: `exports.consumers = {${configAnalysis.consumers.map((c) => `${c.name}: exports.${c.name}Config`).join(', ')}};`
		output += `\n\n// Consumers namespace\n${consumersExport}`
	}

	if (!projectConfig.compilerOptions.target) {
		projectConfig.compilerOptions.target = ts.ScriptTarget.ES2020
	}

	if (!projectConfig.compilerOptions.module) {
		projectConfig.compilerOptions.module = ts.ModuleKind.CommonJS
	}

	// Adicionar header com informações de geração
	const header = `// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from change-stream/config.ts and change-stream/message-payload.schema.ts
// Target: ${ts.ScriptTarget[projectConfig.compilerOptions.target]}
// Module: ${ts.ModuleKind[projectConfig.compilerOptions.module]}
// Generated at: ${new Date().toISOString()}

`
	return header + output
}

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

function generateIndexFile(): string {
	return `// Auto-generated index file
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
}

async function generatePackegeJson(outputDir: string): Promise<void> {
	const packageJsonContent = `{
  "name": "@dafaz/change-stream-broker-client",
  "version": "1.0.0",
  "main": "index.js"
}`

	await fs.writeFile(path.join(outputDir, 'package.json'), packageJsonContent)
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

		const outputDir = path.join(packageDir, '..', 'client')
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

		const clientContentJs = generateJavaScriptClient(
			clientContent,
			configAnalysis,
		)

		const clientDtsContent = generateTypeDefinitions(clientContent)

		await fs.ensureDir(outputDir)

		// arquivo TypeScript gerado (referência)
		// await fs.writeFile(outputPath, clientContent)

		// arquivo JavaScript transpilado
		await fs.writeFile(
			path.join(outputDir, 'broker.client.js'),
			clientContentJs,
		)
		await fs.writeFile(
			path.join(outputDir, 'broker.client.d.ts'),
			clientDtsContent,
		)

		// arquivo index.ts
		const indexContent = generateIndexFile()
		const indexContentJs = generateJavaScriptClient(
			indexContent,
			configAnalysis,
		)
		await fs.writeFile(path.join(outputDir, 'index.js'), indexContentJs)

		// arquivo index.d.ts
		const indexDtsContent = generateTypeDefinitions(indexContent)
		await fs.writeFile(path.join(outputDir, 'index.d.ts'), indexDtsContent)

		console.log('✅ Client generated successfully!')
		console.log(
			'📁 File created: node_modules/@dafaz/change-stream-broker/client/broker.client.ts',
		)

		await generatePackegeJson(outputDir)

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

function generateTypeDefinitions(content: string): string {
	const projectConfig = detectProjectConfig()
	const lib = projectConfig.compilerOptions.lib?.includes('ES2020')
		? 'ES2020'
		: 'ES2018'

	return `
/// <reference lib="${lib}" />

`.concat(content)
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
	// const capitalize = (str: string): string => {
	// 	return str.charAt(0).toUpperCase() + str.slice(1)
	// }

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
try {
	require('dotenv').config({ override: false })
} catch {
	console.warn('⚠️  dotenv not available, using process.env')
}

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
	return `// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from change-stream/config.ts and change-stream/message-payload.schema.ts

import {
	BrokerConfig,
	ConsumerConfig,
	Message,
	ProducerConfig,
} from '@dafaz/change-stream-broker'

// ==============================================
// Configs
// ==============================================

${configSection}

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

// ==============================================
// TYPE UTILITIES
// ==============================================
export type MessageType = keyof MessagePayloads;
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
