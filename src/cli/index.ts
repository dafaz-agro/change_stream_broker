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

	// program
	// 	.command('backups')
	// 	.description('List available client backups')
	// 	.action(async () => {
	// 		try {
	// 			const packageDir = await getPackageDir()
	// 			const clientDir = path.join(packageDir, '..', 'client')

	// 			if (!(await fs.pathExists(clientDir))) {
	// 				console.log('No backups available - client directory does not exist')
	// 				return
	// 			}

	// 			const items = await fs.readdir(clientDir)
	// 			const backupDirs = items.filter(
	// 				(item) =>
	// 					item.startsWith('backup_') &&
	// 					fs.statSync(path.join(clientDir, item)).isDirectory(),
	// 			)

	// 			if (backupDirs.length === 0) {
	// 				console.log('No backups available')
	// 				return
	// 			}

	// 			console.log('📦 Available backups:')
	// 			backupDirs
	// 				.sort()
	// 				.reverse()
	// 				.forEach((backupDir, index) => {
	// 					const timestamp = backupDir.replace('backup_', '')
	// 					const date = parseBackupTimestamp(timestamp)

	// 					console.log(`${index + 1}. ${backupDir} (${date.toLocaleString()})`)
	// 				})
	// 		} catch (error) {
	// 			console.error('Error listing backups:', error)
	// 		}
	// 	})

	// program
	// 	.command('restore <backupName>')
	// 	.description('Restore a specific backup')
	// 	.action(async (backupName) => {
	// 		try {
	// 			const packageDir = await getPackageDir()
	// 			const clientDir = path.join(packageDir, '..', 'client')
	// 			const backupDir = path.join(clientDir, backupName)

	// 			if (!(await fs.pathExists(backupDir))) {
	// 				throw new Error(`Backup ${backupName} not found`)
	// 			}

	// 			// Ler arquivos do diretório de backup
	// 			const backupFiles = await fs.readdir(backupDir)

	// 			// Verificar se temos todos os arquivos necessários
	// 			const requiredFiles = [
	// 				'broker.client.js',
	// 				'broker.client.d.ts',
	// 				'index.js',
	// 				'index.d.ts',
	// 			]

	// 			const hasRequiredFiles = requiredFiles.every((requiredFile) =>
	// 				backupFiles.some((backupFile) => backupFile.startsWith(requiredFile)),
	// 			)

	// 			if (!hasRequiredFiles) {
	// 				throw new Error(`Backup ${backupName} is incomplete or corrupted`)
	// 			}

	// 			// Fazer backup dos arquivos atuais primeiro
	// 			const timestamp = generateTimestamp()
	// 			const currentBackupDir = path.join(clientDir, `backup_${timestamp}`)
	// 			await fs.ensureDir(currentBackupDir)

	// 			// Copiar arquivos atuais para backup
	// 			const currentFiles = (await fs.readdir(clientDir)).filter(
	// 				(file) =>
	// 					(file.endsWith('.js') || file.endsWith('.d.ts')) &&
	// 					!file.startsWith('backup_'), // Não copiar pastas de backup
	// 			)

	// 			for (const file of currentFiles) {
	// 				const sourcePath = path.join(clientDir, file)
	// 				const backupPath = path.join(currentBackupDir, `${file}_${timestamp}`)
	// 				await fs.copy(sourcePath, backupPath)
	// 			}

	// 			// Restaurar arquivos do backup
	// 			for (const backupFile of backupFiles) {
	// 				// Extrair o nome original do arquivo (remover o timestamp)
	// 				const originalFileName = backupFile.split('_').slice(0, -2).join('_')

	// 				const sourcePath = path.join(backupDir, backupFile)
	// 				const targetPath = path.join(clientDir, originalFileName)

	// 				await fs.copy(sourcePath, targetPath)
	// 			}

	// 			console.log('✅ Backup restored successfully!')
	// 			console.log(
	// 				`📁 Current files backed up to: ${path.basename(currentBackupDir)}`,
	// 			)
	// 		} catch (error) {
	// 			console.error('Error restoring backup:', error)
	// 		}
	// 	})

	program
		.command('backups')
		.description('List available backups')
		.action(async () => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')

				if (!(await fs.pathExists(clientDir))) {
					console.log('No backups available')
					return
				}

				const items = await fs.readdir(clientDir)

				const potentialBackupDirs = items.filter((item) =>
					item.startsWith('backup_'),
				)

				const backupDirs: string[] = []

				for (const item of potentialBackupDirs) {
					const itemPath = path.join(clientDir, item)
					const stat = await fs.stat(itemPath)
					if (stat.isDirectory()) {
						backupDirs.push(item)
					}
				}

				if (backupDirs.length === 0) {
					console.log('No backups available')
					return
				}

				backupDirs.sort().reverse()

				console.log('📦 Available backups:')
				for (const backupDir of backupDirs) {
					const timestamp = backupDir.replace('backup_', '')
					try {
						const date = parseBackupTimestamp(timestamp)
						console.log(`   ${backupDir} (${date.toLocaleString()})`)
					} catch {
						// Se formato inválido, mostrar sem data
						console.log(`   ${backupDir} (invalid timestamp: ${timestamp})`)
					}
				}
			} catch (error) {
				console.error('Error listing backups:', error)
			}
		})

	program
		.command('stage')
		.description('Show current change-stream stage content')
		.action(async () => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')

				if (!(await fs.pathExists(stageDir))) {
					console.log('No change-stream stage available')
					return
				}

				const stageFiles = await fs.readdir(stageDir)
				console.log('📁 change-stream_stage content:')
				stageFiles.forEach((file) => {
					console.log(`   - ${file}`)
				})

				// Mostrar conteúdo dos arquivos (opcional)
				if (stageFiles.includes('config.ts')) {
					const configContent = await fs.readFile(
						path.join(stageDir, 'config.ts'),
						'utf-8',
					)
					console.log('\n📋 config.ts preview:')
					console.log(
						configContent.split('\n').slice(0, 10).join('\n').concat('\n...'),
					)
				}
			} catch (error) {
				console.error('Error checking stage:', error)
			}
		})

	program
		.command('restore <backupName>')
		.description('Restore a specific backup (client files + update stage)')
		.action(async (backupName) => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const backupDir = path.join(clientDir, backupName)
				const changeStreamStageDir = path.join(clientDir, 'change-stream_stage')

				if (!(await fs.pathExists(backupDir))) {
					throw new Error(`Backup ${backupName} not found`)
				}

				// Verificar estrutura do backup
				const backupClientDir = path.join(backupDir, 'client')
				const backupChangeStreamDir = path.join(backupDir, 'change-stream')

				if (
					!(await fs.pathExists(backupClientDir)) ||
					!(await fs.pathExists(backupChangeStreamDir))
				) {
					throw new Error('Backup has invalid structure')
				}

				// 1. Fazer backup do estado atual do CLIENT apenas
				const timestamp = generateTimestamp()
				const currentBackupDir = path.join(clientDir, `backup_${timestamp}`)
				await fs.ensureDir(currentBackupDir)

				// Backup apenas dos arquivos do client atual
				if (await fs.pathExists(clientDir)) {
					const currentClientFiles = (await fs.readdir(clientDir)).filter(
						(file) =>
							(file.endsWith('.js') || file.endsWith('.d.ts')) &&
							!file.startsWith('backup_') &&
							!file.startsWith('change-stream_'),
					)

					for (const file of currentClientFiles) {
						const sourcePath = path.join(clientDir, file)
						const backupPath = path.join(currentBackupDir, 'client', file)
						await fs.copy(sourcePath, backupPath)
					}

					const currentChangeStreamStageFiles = (
						await fs.readdir(changeStreamStageDir)
					).filter((file) => file.endsWith('.ts') || file.endsWith('.json'))

					for (const file of currentChangeStreamStageFiles) {
						const sourcePath = path.join(changeStreamStageDir, file)
						const backupPath = path.join(
							currentBackupDir,
							'change-stream',
							file,
						)
						await fs.copy(sourcePath, backupPath)
					}
				}

				console.log(
					`📦 Current client state backed up to: ${path.basename(currentBackupDir)}`,
				)

				// 2. RESTAURAR ARQUIVOS DO CLIENT
				const backupClientFiles = await fs.readdir(backupClientDir)
				for (const file of backupClientFiles) {
					const sourcePath = path.join(backupClientDir, file)
					const targetPath = path.join(clientDir, file)

					await fs.copy(sourcePath, targetPath)
					console.log(`✅ Restored client: ${file}`)
				}

				// 3. ATUALIZAR O STAGE com os arquivos do backup
				const stageDir = path.join(clientDir, 'change-stream_stage')
				await fs.ensureDir(stageDir)
				await fs.emptyDir(stageDir)

				const backupChangeStreamFiles = await fs.readdir(backupChangeStreamDir)
				for (const file of backupChangeStreamFiles) {
					const sourcePath = path.join(backupChangeStreamDir, file)
					const targetPath = path.join(stageDir, file)
					await fs.copy(sourcePath, targetPath)
					console.log(`✅ Updated stage with: ${file}`)
				}

				// 4. CRIAR ARQUIVO DE INSTRUÇÕES para o desenvolvedor
				const instructions = `
# 📋 INSTRUÇÕES DE RESTAURAÇÃO

Backup restaurado: ${backupName}
Data do restore: ${new Date().toLocaleString()}

## ✅ O que foi feito:
1. Arquivos do client restaurados em: node_modules/@dafaz/change-stream-broker/client/
2. Stage atualizado com os arquivos change-stream do backup

## 🚨 Próximos passos MANUAIS:

### Opção 1: Usar arquivos do stage (RECOMENDADO)
\`\`\`bash
# Copiar arquivos do stage para seu change-stream
cp -r node_modules/@dafaz/change-stream-broker/client/change-stream_stage/* change-stream/
\`\`\`

### Opção 2: Comparar e mesclar manualmente
\`\`\`bash
# Ver diferenças entre stage e seu change-stream atual
diff -r node_modules/@dafaz/change-stream-broker/client/change-stream_stage/ change-stream/

# Ou usar ferramenta visual de diff
code --diff node_modules/@dafaz/change-stream-broker/client/change-stream_stage/config.ts change-stream/config.ts
\`\`\`

## 📊 Arquivos disponíveis no stage:
${backupChangeStreamFiles.map((file) => `- ${file}`).join('\n')}

💡 O backup do seu estado atual está em: ${path.basename(currentBackupDir)}
					`

				const instructionsPath = path.join(
					clientDir,
					`RESTORE_INSTRUCTIONS_${timestamp}.md`,
				)
				await fs.writeFile(instructionsPath, instructions)

				console.log('✅ Backup restored successfully!')
				console.log('📁 Client files restored')
				console.log('📁 Stage updated with backup change-stream files')
				console.log(
					'📋 Instructions saved to: ',
					path.basename(instructionsPath),
				)
				console.log(
					'\n🚨 IMPORTANTE: Você precisa manualmente copiar os arquivos do stage para seu change-stream/',
				)
				console.log('   Siga as instruções no arquivo de instruções.')
			} catch (error) {
				console.error('Error restoring backup:', error)
			}
		})
	program
		.command('apply-stage')
		.description('Copy files from stage to change-stream directory')
		.option('--force', 'Overwrite without confirmation')
		.action(async (options) => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')
				const changeStreamDir = path.join(process.cwd(), 'change-stream')

				if (!(await fs.pathExists(stageDir))) {
					throw new Error('No stage available. Run a restore first.')
				}

				if (!(await fs.pathExists(changeStreamDir))) {
					throw new Error(
						'Change-stream directory not found. Run "csbroker init" first.',
					)
				}

				const stageFiles = await fs.readdir(stageDir)
				const changeStreamFiles = await fs.readdir(changeStreamDir)

				// Verificar diferenças
				const diffFiles = []
				for (const file of stageFiles) {
					if (changeStreamFiles.includes(file)) {
						const stageContent = await fs.readFile(
							path.join(stageDir, file),
							'utf-8',
						)
						const currentContent = await fs.readFile(
							path.join(changeStreamDir, file),
							'utf-8',
						)

						if (stageContent !== currentContent) {
							diffFiles.push(file)
						}
					}
				}

				const newFiles = stageFiles.filter(
					(file) => !changeStreamFiles.includes(file),
				)

				if (diffFiles.length === 0 && newFiles.length === 0) {
					console.log(
						'✅ No changes to apply - stage and change-stream are identical',
					)
					return
				}

				// Mostrar preview das mudanças
				console.log('📋 Changes to be applied:')
				if (diffFiles.length > 0) {
					console.log('🔄 Modified files:')
					diffFiles.forEach((file) => {
						console.log(`   - ${file}`)
					})
				}
				if (newFiles.length > 0) {
					console.log('🆕 New files:')
					newFiles.forEach((file) => {
						console.log(`   - ${file}`)
					})
				}

				// Confirmação (a menos que --force)
				if (!options.force) {
					const readline = require('node:readline').createInterface({
						input: process.stdin,
						output: process.stdout,
					})

					const answer: string = await new Promise((resolve) => {
						readline.question(
							'\n❓ Apply these changes to change-stream/? (y/N) ',
							resolve,
						)
					})
					readline.close()

					if (answer.toLowerCase() !== 'y') {
						console.log('❌ Operation cancelled')
						return
					}
				}

				// Aplicar mudanças
				for (const file of stageFiles) {
					const sourcePath = path.join(stageDir, file)
					const targetPath = path.join(changeStreamDir, file)
					await fs.copy(sourcePath, targetPath)
					console.log(`✅ Applied: ${file}`)
				}

				console.log('✅ Stage applied successfully!')
				console.log(
					'💡 You may want to run: npx csbroker generate to ensure consistency',
				)
			} catch (error) {
				console.error('Error applying stage:', error)
			}
		})

	program
		.command('diff')
		.description('Show differences between current change-stream and stage')
		.action(async () => {
			try {
				const currentDir = path.join(process.cwd(), 'change-stream')
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')

				if (!(await fs.pathExists(currentDir))) {
					console.log('Current change-stream directory not found')
					return
				}

				if (!(await fs.pathExists(stageDir))) {
					console.log('No stage available')
					return
				}

				const currentFiles = await fs.readdir(currentDir)
				const stageFiles = await fs.readdir(stageDir)

				console.log('🔍 Comparing change-stream with stage:')
				console.log(`   Current: ${currentFiles.length} files`)
				console.log(`   Stage: ${stageFiles.length} files`)

				// Verificar diferenças nos arquivos principais
				const importantFiles = ['config.ts', 'message-payload.schema.ts']

				for (const file of importantFiles) {
					const currentPath = path.join(currentDir, file)
					const stagePath = path.join(stageDir, file)

					const currentExists = await fs.pathExists(currentPath)
					const stageExists = await fs.pathExists(stagePath)

					if (currentExists && stageExists) {
						const currentContent = await fs.readFile(currentPath, 'utf-8')
						const stageContent = await fs.readFile(stagePath, 'utf-8')

						if (currentContent === stageContent) {
							console.log(`   ✅ ${file}: No changes`)
						} else {
							console.log(`   ⚠️  ${file}: Modified`)
						}
					} else if (currentExists && !stageExists) {
						console.log(`   ❌ ${file}: Added in current`)
					} else if (!currentExists && stageExists) {
						console.log(`   ❌ ${file}: Removed from current`)
					}
				}
			} catch (error) {
				console.error('Error comparing changes:', error)
			}
		})
	program
		.command('compare-stage')
		.description('Compare stage with current change-stream files')
		.action(async () => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')
				const changeStreamDir = path.join(process.cwd(), 'change-stream')

				if (!(await fs.pathExists(stageDir))) {
					throw new Error('No stage available')
				}

				if (!(await fs.pathExists(changeStreamDir))) {
					throw new Error('Change-stream directory not found')
				}

				const stageFiles = await fs.readdir(stageDir)
				const changeStreamFiles = await fs.readdir(changeStreamDir)

				console.log('🔍 Comparing stage vs change-stream:')
				console.log(`   Stage: ${stageFiles.length} files`)
				console.log(`   Change-stream: ${changeStreamFiles.length} files`)

				const onlyInStage = stageFiles.filter(
					(file) => !changeStreamFiles.includes(file),
				)
				const onlyInChangeStream = changeStreamFiles.filter(
					(file) => !stageFiles.includes(file),
				)
				const commonFiles = stageFiles.filter((file) =>
					changeStreamFiles.includes(file),
				)

				if (onlyInStage.length > 0) {
					console.log('\n📁 Only in stage:')
					onlyInStage.forEach((file) => {
						console.log(`   - ${file} (will be added)`)
					})
				}

				if (onlyInChangeStream.length > 0) {
					console.log('\n📁 Only in change-stream:')
					onlyInChangeStream.forEach((file) => {
						console.log(`   - ${file} (will be kept)`)
					})
				}

				if (commonFiles.length > 0) {
					console.log('\n📁 Common files (comparing content):')
					let diffCount = 0
					for (const file of commonFiles) {
						const stageContent = await fs.readFile(
							path.join(stageDir, file),
							'utf-8',
						)
						const changeStreamContent = await fs.readFile(
							path.join(changeStreamDir, file),
							'utf-8',
						)

						if (stageContent !== changeStreamContent) {
							console.log(`   - ${file} (DIFFERENT - will be overwritten)`)
							diffCount++
						} else {
							console.log(`   - ${file} (identical)`)
						}
					}

					if (diffCount > 0) {
						console.log(
							`\n⚠️  ${diffCount} files will be overwritten if you apply the stage`,
						)
					}
				}

				console.log('\n💡 Use "csbroker apply-stage" to apply these changes')
				console.log(
					'💡 Use "csbroker apply-stage --force" to apply without confirmation',
				)
			} catch (error) {
				console.error('Error comparing:', error)
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
