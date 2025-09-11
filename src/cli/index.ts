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
	name: string
	path: string
	timestamp: Date
	formattedDate: string
	fileCount: number
}

function parseBackupTimestamp(timestamp: string): Date {
	// Formato esperado: YYYYMMDD_HHmmss
	if (timestamp.length !== 15 || timestamp.charAt(8) !== '_') {
		throw new Error(`Invalid timestamp format: ${timestamp}`)
	}

	return new Date(
		parseInt(timestamp.slice(0, 4), 10), // year
		parseInt(timestamp.slice(4, 6), 10) - 1, // month (0-indexed)
		parseInt(timestamp.slice(6, 8), 10), // day
		parseInt(timestamp.slice(9, 11), 10), // hour
		parseInt(timestamp.slice(11, 13), 10), // minute
		parseInt(timestamp.slice(13, 15), 10), // second
	)
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
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')

				if (!(await fs.pathExists(clientDir))) {
					console.log('No backups available - client directory does not exist')
					return
				}

				const items = await fs.readdir(clientDir)
				const backupDirs = items.filter(
					(item) =>
						item.startsWith('backup_') &&
						fs.statSync(path.join(clientDir, item)).isDirectory(),
				)

				if (backupDirs.length === 0) {
					console.log('No backups available')
					return
				}

				console.log('📦 Available backups:')
				backupDirs
					.sort()
					.reverse()
					.forEach((backupDir, index) => {
						const timestamp = backupDir.replace('backup_', '')
						const date = parseBackupTimestamp(timestamp)

						console.log(`${index + 1}. ${backupDir} (${date.toLocaleString()})`)
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
				const clientDir = path.join(packageDir, '..', 'client')
				const backupDir = path.join(clientDir, backupName)

				if (!(await fs.pathExists(backupDir))) {
					throw new Error(`Backup ${backupName} not found`)
				}

				// Verificar se é um diretório de backup válido
				const filesWithTimestamp = await fs.readdir(backupDir)

				const originalFiles = filesWithTimestamp.map(
					(file) => file.split('_')[0],
				)

				const requiredFiles = [
					'broker.client.js',
					'broker.client.d.ts',
					'index.js',
					'index.d.ts',
				]

				const hasRequiredFiles = requiredFiles.every((file) =>
					originalFiles.includes(file),
				)

				if (!hasRequiredFiles) {
					throw new Error(`Backup ${backupName} is incomplete or corrupted`)
				}

				// Fazer backup dos arquivos atuais primeiro
				const timestamp = generateTimestamp()
				const currentBackupDir = path.join(
					clientDir,
					`backup_before_restore_${timestamp}`,
				)
				await fs.ensureDir(currentBackupDir)

				// Copiar arquivos atuais para backup
				const currentFiles = (await fs.readdir(clientDir)).filter(
					(file) => file.endsWith('.js') || file.endsWith('.d.ts'),
				)

				for (const file of currentFiles) {
					const sourcePath = path.join(clientDir, file)
					const backupPath = path.join(currentBackupDir, file)
					await fs.copy(sourcePath, backupPath)
				}

				// Restaurar arquivos do backup
				for (const file of filesWithTimestamp) {
					const sourcePath = path.join(backupDir, file)
					const originalFileName = file.split('_')[0] // Remover timestamp

					if (!originalFileName) {
						throw new Error(`Backup ${backupName} is incomplete or corrupted`)
					}

					const targetPath = path.join(clientDir, originalFileName)

					if (file.endsWith('.js') || file.endsWith('.d.ts')) {
						await fs.copy(sourcePath, targetPath)
						console.log(`✅ Restored: ${file}`)
					}
				}

				console.log('✅ Backup restored successfully!')
				console.log(
					`📁 Current files backed up to: ${path.basename(currentBackupDir)}`,
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
	if (error instanceof Error) {
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
	} else {
		console.error('❌ Unknown error:', error)
		process.exit(1)
	}
}
