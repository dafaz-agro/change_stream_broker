#!/usr/bin/env node

import path, { join } from 'node:path'
import { Command } from 'commander'
import fs, { existsSync } from 'fs-extra'
import {
	generateClient,
	generateTimestamp,
	getPackageDir,
} from './commands/generate'
import { initConfiguration } from './commands/init'
import { watchForChanges } from './commands/watch'

export interface ClientBackup {
	filename: string
	timestamp: Date
	formattedDate: string
}

try {
	// Carregar apenas .env de desenvolvimento
	const envPath = join(process.cwd(), '.env')
	const envDevelopment = join(process.cwd(), '.env.development')
	const envLocal = join(process.cwd(), '.env.local')

	// Prioridade: .env.local > .env.development > .env
	if (existsSync(envLocal)) {
		require('dotenv').config({ path: envLocal })
	} else if (existsSync(envDevelopment)) {
		require('dotenv').config({ path: envDevelopment })
	} else if (existsSync(envPath)) {
		require('dotenv').config({ path: envPath })
	}
} catch (error) {
	if (error instanceof Error) {
		console.warn('⚠️  Could not load .env files:', error.message)
	}
}

function isDockerEnvironment(): boolean {
	return (
		existsSync('/.dockerenv') ||
		existsSync('/run/.containerenv') ||
		process.env.CONTAINER === 'true' ||
		process.env.DOCKER === 'true'
	)
}

function showProductionError(reason: string): void {
	console.error('🚫 SECURITY RESTRICTION - PRODUCTION ENVIRONMENT DETECTED')
	console.error(`   Reason: ${reason}`)
	console.error('')
	console.error(
		'💡 The Change Stream Broker CLI is STRICTLY disabled in production environments.',
	)
	console.error('')
	console.error('📋 DEVELOPMENT USAGE:')
	console.error('   # Ensure NODE_ENV=development')
	console.error('   echo "NODE_ENV=development" > .env')
	console.error('')
	console.error('   # Generate client in development')
	console.error('   npx csbroker generate')
	console.error('')
	console.error('   # Build your application (includes client generation)')
	console.error('   npm run build')
	console.error('')
	console.error('🔒 SECURITY POLICY:')
	console.error('   - No --force flag available')
	console.error('   - No CI override in production')
	console.error('   - Strict environment detection')
	console.error('')
	console.error('📞 Contact the development team if this is a false positive.')
}

function checkEnvironment(): void {
	// 1. Verificar NODE_ENV via dotenv
	const nodeEnv = process.env.NODE_ENV || 'development'

	if (nodeEnv === 'production') {
		showProductionError('NODE_ENV=production detected')
		process.exit(1)
	}

	// 2. Verificar se é ambiente Docker/Container
	if (isDockerEnvironment()) {
		showProductionError('Docker/container environment detected')
		process.exit(1)
	}

	// 3. 🔒 NUNCA permitir CI=true em produção (segurança extra)
	if (process.env.CI === 'true' && nodeEnv === 'production') {
		showProductionError('CI execution in production is not allowed')
		process.exit(1)
	}
}

try {
	checkEnvironment()

	const program = new Command()

	program
		.name('csbroker')
		.description('CLI for Change Stream Broker management')
		.version('1.0.0')

	program
		.command('init')
		.description('Initialize Change Stream Broker configuration')
		.action(() => {
			initConfiguration().catch((error) => {
				console.error('Failed to initialize:', error)
				process.exit(1)
			})
		})

	program
		.command('generate')
		.description('Generate broker client with configured producers/consumers')
		.action(() => {
			generateClient().catch((error) => {
				console.error('Failed to generate:', error)
				process.exit(1)
			})
		})

	program
		.command('watch')
		.description('Watch for changes and regenerate client')
		.action(() => {
			watchForChanges().catch(console.error)
		})

	program
		.command('backups')
		.description('List available client backups')
		.action(async () => {
			try {
				const {
					listClientBackups,
				} = require('@dafaz/change-stream-broker/generated')
				const backups: ClientBackup[] = await listClientBackups()

				if (backups.length === 0) {
					console.log('No backups available')
					return
				}

				console.log('📦 Available backups:')
				backups.forEach((backup, index) => {
					const dateStr = backup.filename
						.replace('broker.client.ts_', '')
						.replace('_', ' ')
					console.log(`${index + 1}. ${backup.filename} (${dateStr})`)
				})
			} catch (error) {
				console.error('Error listing backups:', error)
			}
		})

	program
		.command('restore <backupName>')
		.description('Restore a specific backup')
		.action(async (backupName) => {
			try {
				const packageDir = await getPackageDir()
				const generatedDir = path.join(packageDir, 'generated')
				const backupPath = path.join(generatedDir, backupName)
				const currentPath = path.join(generatedDir, 'broker.client.ts')

				if (!(await fs.pathExists(backupPath))) {
					throw new Error(`Backup ${backupName} not found`)
				}

				// Fazer backup do atual primeiro
				const timestamp = generateTimestamp()
				await fs.copy(currentPath, `${currentPath}_${timestamp}`)

				// Restaurar backup
				await fs.copy(backupPath, currentPath)

				console.log('✅ Backup restored successfully!')
				console.log(
					`📁 Current version backed up as: broker.client.ts_${timestamp}`,
				)
			} catch (error) {
				console.error('Error restoring backup:', error)
			}
		})

	program.parse()
} catch (error) {
	handleError(error)
}

function handleError(error: any): void {
	if (
		error.message.includes('production') ||
		error.message.includes('disabled')
	) {
		console.error('❌ Security violation detected')
		process.exit(1)
	} else {
		console.error('❌ Error:', error.message)
		process.exit(1)
	}
}
