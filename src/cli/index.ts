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

	// program
	// 	.command('stage')
	// 	.description('Show current change-stream stage content')
	// 	.action(async () => {
	// 		try {
	// 			const packageDir = await getPackageDir()
	// 			const clientDir = path.join(packageDir, '..', 'client')
	// 			const stageDir = path.join(clientDir, 'change-stream_stage')

	// 			if (!(await fs.pathExists(stageDir))) {
	// 				console.log('No change-stream stage available')
	// 				return
	// 			}

	// 			const stageFiles = await fs.readdir(stageDir)
	// 			console.log('📁 change-stream_stage content:')
	// 			stageFiles.forEach((file) => {
	// 				console.log(`   - ${file}`)
	// 			})

	// 			// Mostrar conteúdo dos arquivos (opcional)
	// 			if (stageFiles.includes('config.ts')) {
	// 				const configContent = await fs.readFile(
	// 					path.join(stageDir, 'config.ts'),
	// 					'utf-8',
	// 				)
	// 				console.log('\n📋 config.ts preview:')
	// 				console.log(
	// 					configContent.split('\n').slice(0, 10).join('\n').concat('\n...'),
	// 				)
	// 			}
	// 		} catch (error) {
	// 			console.error('Error checking stage:', error)
	// 		}
	// 	})

	program
		.command('stage')
		.description('Interactive stage file viewer')
		.option('-f, --file <filename>', 'View a specific file directly')
		.action(async (options) => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')

				if (!(await fs.pathExists(stageDir))) {
					console.log('❌ No change-stream stage available')
					console.log('💡 Run "npx csbroker generate" first to create a stage')
					return
				}

				const stageFiles = (await fs.readdir(stageDir)).filter(
					(file) =>
						file.endsWith('.ts') ||
						file.endsWith('.js') ||
						file.endsWith('.json'),
				)

				if (stageFiles.length === 0) {
					console.log('📁 Stage directory is empty')
					return
				}

				// Se o usuário especificou um arquivo diretamente
				if (options.file) {
					if (stageFiles.includes(options.file)) {
						await displayFileContent(
							path.join(stageDir, options.file),
							options.file,
						)
					} else {
						console.log(`❌ File not found: ${options.file}`)
						console.log('📁 Available files:')
						stageFiles.forEach((file) => {
							console.log(`   - ${file}`)
						})
					}
					return
				}

				// Menu interativo
				await showInteractiveMenu(stageDir, stageFiles)
			} catch (error) {
				console.error('Error in stage command:', error)
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

					const metaData = {
						timestamp: timestamp,
						backedUpAt: new Date().toISOString(),
						clientFiles: currentClientFiles,
						changeStreamFiles: currentChangeStreamStageFiles,
						sourceDirectory: process.cwd(),
					}

					await fs.writeFile(
						path.join(currentBackupDir, 'client', '.metadata.json'),
						JSON.stringify(metaData, null, 2),
					)
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

				console.log(
					'\n🚨 IMPORTANTE: Você precisa verificar os arquivos de stage para seu change-stream:\n',
				)
				console.log(
					'  1. Utilize o comando (npx csbroker diff) para fazer a verificação.\n',
				)
				console.log(
					'  2. Utilize o comando (npx csbroker stage) para ler os arquivos de stage do change_stream/.\n',
				)
				console.log(
					'  3. Utilize o comando (npx csbroker apply-stage) para aplicar as modificações seletivamente, e atualizar o seu change-stream/ conforme sua necessidade.\n',
				)
			} catch (error) {
				console.error('Error restoring backup:', error)
			}
		})
	program
		.command('apply-stage')
		.description('Interactive stage file applicator')
		.option('--force', 'Apply all changes without confirmation')
		.option('--dry-run', 'Show what would be applied without making changes')
		.action(async (options) => {
			try {
				const packageDir = await getPackageDir()
				const clientDir = path.join(packageDir, '..', 'client')
				const stageDir = path.join(clientDir, 'change-stream_stage')
				const changeStreamDir = path.join(process.cwd(), 'change-stream')

				if (!(await fs.pathExists(stageDir))) {
					throw new Error(
						'No stage available. Run a restore or generate first.',
					)
				}

				if (!(await fs.pathExists(changeStreamDir))) {
					throw new Error(
						'Change-stream directory not found. Run "csbroker init" first.',
					)
				}

				// Ler arquivos do stage (ignorando metafiles)
				const stageFiles = (await fs.readdir(stageDir)).filter(
					(file) => !isMetaFile(file),
				)

				if (stageFiles.length === 0) {
					console.log('📁 Stage directory is empty (no files to apply)')
					return
				}

				// Detectar diferenças
				const {
					modified,
					new: newFiles,
					identical,
				} = await getFileDifferences(stageDir, changeStreamDir, stageFiles)

				if (modified.length === 0 && newFiles.length === 0) {
					console.log(
						'✅ No changes to apply - stage and change-stream are identical',
					)
					if (identical.length > 0) {
						console.log(`📁 Identical files: ${identical.join(', ')}`)
					}
					return
				}

				// Mostrar resumo
				console.log('📋 Changes detected:')
				if (modified.length > 0) {
					console.log(
						`🔄 Modified: ${modified.length} file(s) - ${modified.join(', ')}`,
					)
				}
				if (newFiles.length > 0) {
					console.log(
						`🆕 New: ${newFiles.length} file(s) - ${newFiles.join(', ')}`,
					)
				}
				if (identical.length > 0) {
					console.log(`✅ Identical: ${identical.length} file(s)`)
				}

				// Modo --force: aplicar tudo automaticamente
				if (options.force) {
					console.log('\n⚡ FORCE MODE: Applying all changes...')
					for (const file of stageFiles) {
						if (isMetaFile(file)) continue
						const sourcePath = path.join(stageDir, file)
						const targetPath = path.join(changeStreamDir, file)
						await fs.copy(sourcePath, targetPath)
						console.log(`✅ Applied: ${file}`)
					}
					console.log('✅ All changes applied successfully!')
					return
				}

				// Modo --dry-run: apenas mostrar
				if (options.dryRun) {
					console.log('\n🔍 DRY RUN: The following changes would be applied:')
					modified.forEach((file) => {
						console.log(`   📝 Modify: ${file}`)
					})
					newFiles.forEach((file) => {
						console.log(`   🆕 Create: ${file}`)
					})
					console.log('\n💡 Use without --dry-run to actually apply changes')
					return
				}

				// Menu interativo
				const filesToApply = await showInteractiveApplyMenu(
					stageDir,
					changeStreamDir,
					modified,
					newFiles,
				)

				if (filesToApply.length === 0) {
					console.log('❌ No files selected for application')
					return
				}

				// Aplicar arquivos selecionados
				console.log('\n🔄 Applying selected files...')
				for (const file of filesToApply) {
					const sourcePath = path.join(stageDir, file)
					const targetPath = path.join(changeStreamDir, file)
					await fs.copy(sourcePath, targetPath)
					console.log(`✅ Applied: ${file}`)
				}

				console.log('\n✅ Stage applied successfully!')
				console.log(`📁 Applied ${filesToApply.length} file(s)`)
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
	// program
	// 	.command('compare-stage')
	// 	.description('Compare stage with current change-stream files')
	// 	.action(async () => {
	// 		try {
	// 			const packageDir = await getPackageDir()
	// 			const clientDir = path.join(packageDir, '..', 'client')
	// 			const stageDir = path.join(clientDir, 'change-stream_stage')
	// 			const changeStreamDir = path.join(process.cwd(), 'change-stream')

	// 			if (!(await fs.pathExists(stageDir))) {
	// 				throw new Error('No stage available')
	// 			}

	// 			if (!(await fs.pathExists(changeStreamDir))) {
	// 				throw new Error('Change-stream directory not found')
	// 			}

	// 			const stageFiles = await fs.readdir(stageDir)
	// 			const changeStreamFiles = await fs.readdir(changeStreamDir)

	// 			console.log('🔍 Comparing stage vs change-stream:')
	// 			console.log(`   Stage: ${stageFiles.length} files`)
	// 			console.log(`   Change-stream: ${changeStreamFiles.length} files`)

	// 			const onlyInStage = stageFiles.filter(
	// 				(file) => !changeStreamFiles.includes(file),
	// 			)
	// 			const onlyInChangeStream = changeStreamFiles.filter(
	// 				(file) => !stageFiles.includes(file),
	// 			)
	// 			const commonFiles = stageFiles.filter((file) =>
	// 				changeStreamFiles.includes(file),
	// 			)

	// 			if (onlyInStage.length > 0) {
	// 				console.log('\n📁 Only in stage:')
	// 				onlyInStage.forEach((file) => {
	// 					console.log(`   - ${file} (will be added)`)
	// 				})
	// 			}

	// 			if (onlyInChangeStream.length > 0) {
	// 				console.log('\n📁 Only in change-stream:')
	// 				onlyInChangeStream.forEach((file) => {
	// 					console.log(`   - ${file} (will be kept)`)
	// 				})
	// 			}

	// 			if (commonFiles.length > 0) {
	// 				console.log('\n📁 Common files (comparing content):')
	// 				let diffCount = 0
	// 				for (const file of commonFiles) {
	// 					const stageContent = await fs.readFile(
	// 						path.join(stageDir, file),
	// 						'utf-8',
	// 					)
	// 					const changeStreamContent = await fs.readFile(
	// 						path.join(changeStreamDir, file),
	// 						'utf-8',
	// 					)

	// 					if (stageContent !== changeStreamContent) {
	// 						console.log(`   - ${file} (DIFFERENT - will be overwritten)`)
	// 						diffCount++
	// 					} else {
	// 						console.log(`   - ${file} (identical)`)
	// 					}
	// 				}

	// 				if (diffCount > 0) {
	// 					console.log(
	// 						`\n⚠️  ${diffCount} files will be overwritten if you apply the stage`,
	// 					)
	// 				}
	// 			}

	// 			console.log('\n💡 Use "csbroker apply-stage" to apply these changes')
	// 			console.log(
	// 				'💡 Use "csbroker apply-stage --force" to apply without confirmation',
	// 			)
	// 		} catch (error) {
	// 			console.error('Error comparing:', error)
	// 		}
	// 	})

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

// Função para mostrar o conteúdo completo do arquivo
async function displayFileContent(
	filePath: string,
	filename: string,
): Promise<void> {
	try {
		const content = await fs.readFile(filePath, 'utf-8')
		console.clear()
		console.log('═'.repeat(80))
		console.log(`📄 ${filename}`)
		console.log('═'.repeat(80))
		console.log(content)
		console.log('═'.repeat(80))
		console.log('Press (ctrl + c) to continue...')

		// Esperar usuário pressionar uma tecla
		process.stdin.setRawMode(true)
		process.stdin.resume()
		await new Promise((resolve) => process.stdin.once('data', resolve))
		process.stdin.setRawMode(false)
	} catch (error) {
		console.error(`Error reading file ${filename}:`, error)
	}
}

// Função para o menu interativo
async function showInteractiveMenu(
	stageDir: string,
	stageFiles: string[],
): Promise<void> {
	let inMenu = true

	while (inMenu) {
		console.clear()
		console.log('⭐ CHANGE-STREAM STAGE MANAGER')
		console.log('═'.repeat(40))
		console.log('📁 Available files:')

		stageFiles.forEach((file, index) => {
			console.log(`   ${index + 1}. ${file}`)
		})

		console.log('═'.repeat(40))
		console.log('   v. View all files sequentially')
		console.log('   q. Quit')
		console.log('═'.repeat(40))

		const readline = require('node:readline').createInterface({
			input: process.stdin,
			output: process.stdout,
		})

		const answer = await new Promise<string>((resolve) => {
			readline.question('\nSelect an option (number, v, or q): ', resolve)
		})
		readline.close()

		const choice = answer.trim().toLowerCase()

		if (choice === 'q') {
			inMenu = false
			console.log('👋 Goodbye!')
			continue
		}

		if (choice === 'v') {
			// Visualizar todos os arquivos sequencialmente
			for (const file of stageFiles) {
				await displayFileContent(path.join(stageDir, file), file)
			}
			continue
		}

		const fileIndex = parseInt(choice, 10) - 1
		if (
			!Number.isNaN(fileIndex) &&
			fileIndex >= 0 &&
			fileIndex < stageFiles.length
		) {
			const selectedFile = stageFiles[fileIndex] || ''
			await displayFileContent(path.join(stageDir, selectedFile), selectedFile)
		} else {
			console.log('❌ Invalid option. Press (ctrl + c) to continue...')
			process.stdin.setRawMode(true)
			process.stdin.resume()
			await new Promise((resolve) => process.stdin.once('data', resolve))
			process.stdin.setRawMode(false)
		}
	}
}

// Função para ignorar metafiles
function isMetaFile(filename: string): boolean {
	const metaFiles = ['.metadata.json', '.DS_Store', 'Thumbs.db', '.gitignore']
	return metaFiles.includes(filename) || filename.startsWith('.')
}

// Função para detectar diferenças entre arquivos
async function getFileDifferences(
	stageDir: string,
	changeStreamDir: string,
	stageFiles: string[],
): Promise<{
	modified: string[]
	new: string[]
	identical: string[]
}> {
	const modified: string[] = []
	const newFiles: string[] = []
	const identical: string[] = []

	// CORREÇÃO: Especificar o tipo explicitamente
	const changeStreamFiles: string[] = (await fs
		.readdir(changeStreamDir)
		.catch(() => [])) as string[]

	for (const file of stageFiles) {
		if (isMetaFile(file)) continue // Ignorar metafiles

		const stagePath = path.join(stageDir, file)
		const changeStreamPath = path.join(changeStreamDir, file)

		// CORREÇÃO: Agora changeStreamFiles é explicitamente string[]
		if (!changeStreamFiles.includes(file)) {
			newFiles.push(file)
			continue
		}

		try {
			const stageContent = await fs.readFile(stagePath, 'utf-8')
			const currentContent = await fs.readFile(changeStreamPath, 'utf-8')

			if (stageContent === currentContent) {
				identical.push(file)
			} else {
				modified.push(file)
			}
		} catch (error) {
			console.warn(
				`⚠️  Could not compare file ${file}:`,
				error instanceof Error ? error.message : String(error),
			)
			modified.push(file) // Assume modified if error reading
		}
	}

	return { modified, new: newFiles, identical }
}

// Função para mostrar diff entre arquivos
async function showFileDiff(
	stageDir: string,
	changeStreamDir: string,
	filename: string,
): Promise<void> {
	try {
		const stageContent = await fs.readFile(
			path.join(stageDir, filename),
			'utf-8',
		)
		const currentContent = await fs.readFile(
			path.join(changeStreamDir, filename),
			'utf-8',
		)

		console.log('\n')
		console.log('═'.repeat(80))
		console.log(`🔍 DIFF: ${filename}`)
		console.log('═'.repeat(80))

		const stageLines = stageContent.split('\n')
		const currentLines = currentContent.split('\n')

		// Mostrar preview das diferenças
		for (let i = 0; i < Math.max(stageLines.length, currentLines.length); i++) {
			const stageLine = stageLines[i] || ''
			const currentLine = currentLines[i] || ''

			if (stageLine !== currentLine) {
				console.log(`Line ${i + 1}:`)
				if (currentLine) console.log(`  CURRENT: ${currentLine}`)
				if (stageLine) console.log(`  STAGE:   ${stageLine}`)
				console.log('')
			}

			// Limitar a mostrar apenas as primeiras 10 diferenças
			if (i >= 10) {
				console.log('... more differences ...')
				break
			}
		}

		console.log('═'.repeat(80))
	} catch (error) {
		console.error(`Error showing diff for ${filename}:`, error)
	}
}

async function showInteractiveApplyMenu(
	stageDir: string,
	changeStreamDir: string,
	modifiedFiles: string[],
	newFiles: string[],
): Promise<string[]> {
	const selectedFiles: string[] = []
	let inMenu = true

	while (inMenu) {
		console.clear()
		console.log('⭐ APPLY STAGE - SELECT FILES')
		console.log('═'.repeat(50))

		// Mostrar arquivos modificados
		if (modifiedFiles.length > 0) {
			console.log('\n🔄 MODIFIED FILES:')
			modifiedFiles.forEach((file, index) => {
				const isSelected = selectedFiles.includes(file)
				console.log(`   ${isSelected ? '✅' : '☐'} ${index + 1}. ${file}`)
			})
		}

		// Mostrar novos arquivos
		if (newFiles.length > 0) {
			console.log('\n🆕 NEW FILES:')
			newFiles.forEach((file, index) => {
				const isSelected = selectedFiles.includes(file)
				const displayIndex = index + modifiedFiles.length + 1
				console.log(`   ${isSelected ? '✅' : '☐'} ${displayIndex}. ${file}`)
			})
		}

		console.log('   d. Show diff for selected file')
		console.log('   a. Select all files')
		console.log('   n. Select none')
		console.log('   c. Confirm and apply selected')
		console.log('   q. Quit without applying')
		console.log('═'.repeat(50))
		console.log(`   Selected: ${selectedFiles.length} file(s)`)
		console.log('═'.repeat(50))

		const readline = require('node:readline').createInterface({
			input: process.stdin,
			output: process.stdout,
		})

		const answer = await new Promise<string>((resolve) => {
			readline.question('\nSelect option (number, d, a, n, c, q): ', resolve)
		})
		readline.close()

		const choice = answer.trim().toLowerCase()
		const allFiles = [...modifiedFiles, ...newFiles]

		// Processar escolha
		if (choice === 'q') {
			inMenu = false
			console.log('❌ Operation cancelled')
			return []
		}

		if (choice === 'c') {
			inMenu = false
			if (selectedFiles.length === 0) {
				console.log('❌ No files selected')
				return []
			}
			return selectedFiles
		}

		if (choice === 'a') {
			selectedFiles.push(...allFiles)
			console.log('✅ All files selected')
			await new Promise((resolve) => setTimeout(resolve, 1000))
			continue
		}

		if (choice === 'n') {
			selectedFiles.length = 0
			console.log('✅ All files deselected')
			await new Promise((resolve) => setTimeout(resolve, 1000))
			continue
		}

		if (choice === 'd') {
			if (selectedFiles.length !== 1) {
				console.log('❌ Please select exactly one file to show diff')
				await new Promise((resolve) => setTimeout(resolve, 2000))
				continue
			}

			if (!selectedFiles[0]) {
				throw new Error('Selected file is undefined')
			}

			await showFileDiff(stageDir, changeStreamDir, selectedFiles[0])
			console.log('Press any key to continue...')
			process.stdin.setRawMode(true)
			process.stdin.resume()
			await new Promise((resolve) => process.stdin.once('data', resolve))
			process.stdin.setRawMode(false)
			continue
		}

		// Seleção numérica de arquivo
		const fileIndex = parseInt(choice, 10) - 1
		if (
			!Number.isNaN(fileIndex) &&
			fileIndex >= 0 &&
			fileIndex < allFiles.length
		) {
			const selectedFile = allFiles[fileIndex]

			if (!selectedFile) {
				throw new Error('Selected file is undefined')
			}

			if (selectedFiles.includes(selectedFile)) {
				// Desselecionar
				selectedFiles.splice(selectedFiles.indexOf(selectedFile), 1)
				console.log(`❌ Deselected: ${selectedFile}`)
			} else {
				// Selecionar
				selectedFiles.push(selectedFile)
				console.log(`✅ Selected: ${selectedFile}`)
			}

			await new Promise((resolve) => setTimeout(resolve, 500))
		} else {
			console.log('❌ Invalid option')
			await new Promise((resolve) => setTimeout(resolve, 1000))
		}
	}

	return selectedFiles
}
