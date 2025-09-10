// src/cli/init.ts

import path from 'node:path'
import fs from 'fs-extra'

export async function initConfiguration(
	targetDir: string = process.cwd(),
): Promise<void> {
	try {
		const changeStreamDir = path.join(targetDir, 'change-stream')
		await fs.ensureDir(changeStreamDir)

		// Caminhos dos templates
		const templatesDir = path.join(__dirname, '..', 'templates')
		const configTemplatePath = path.join(templatesDir, 'config.ts.template')
		const schemaTemplatePath = path.join(
			templatesDir,
			'message-payload.schema.ts.template',
		)

		// Verificar se os templates existem
		if (!(await fs.pathExists(configTemplatePath))) {
			throw new Error(`Template not found: ${configTemplatePath}`)
		}

		if (!(await fs.pathExists(schemaTemplatePath))) {
			throw new Error(`Template not found: ${schemaTemplatePath}`)
		}

		// Ler templates
		const configTemplate = await fs.readFile(configTemplatePath, 'utf-8')
		const schemaTemplate = await fs.readFile(schemaTemplatePath, 'utf-8')

		// Criar config.ts
		await fs.writeFile(path.join(changeStreamDir, 'config.ts'), configTemplate)

		// Criar message-payload.schema.ts
		await fs.writeFile(
			path.join(changeStreamDir, 'message-payload.schema.ts'),
			schemaTemplate,
		)

		console.log('✅ Change Stream Broker configuration initialized!')
		console.log('📁 Files created:')
		console.log('   - change-stream/config.ts')
		console.log('   - change-stream/message-payload.schema.ts')
		console.log('')
		console.log('📝 Next steps:')
		console.log('   1. Review and customize the generated files')
		console.log('   2. Run: npx csbroker generate')
		console.log('   3. Import from: @dafaz/change-stream-broker/generated')
	} catch (error) {
		console.error('❌ Failed to initialize configuration:')

		// Tratamento seguro de erro
		if (error instanceof Error) {
			console.error(error.message)

			// Verificar se é um erro do sistema de arquivos
			if ('code' in error && typeof error.code === 'string') {
				if (error.code === 'ENOENT') {
					console.log('')
					console.log(
						'💡 Dica: Certifique-se de que os templates foram copiados durante o build:',
					)
					console.log('   - Execute: npm run build')
					console.log('   - Verifique se a pasta dist/cli/templates/ existe')
				}
			}
		} else {
			console.error('Unknown error:', error)
		}

		process.exit(1)
	}
}
