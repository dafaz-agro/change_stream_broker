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

		// Criar arquivo README com instruções
		const readmeContent = `# Change Stream Broker Configuration

Esta pasta contém a configuração do Change Stream Broker para sua aplicação.

## Arquivos:

- \`config.ts\` - Configuração do broker, tópicos, producers e consumers
- \`message-payload.schema.ts\` - Schemas TypeScript para as mensagens (OBRIGATÓRIO)

## Como usar:

1. Edite os schemas em \`message-payload.schema.ts\` conforme suas necessidades
2. Configure os tópicos, producers e consumers em \`config.ts\`
3. Execute \`npx csbroker generate\` para gerar o cliente
4. Use o cliente em sua aplicação:

\`\`\`typescript
import { brokerClient } from './change-stream/client'
import { UserCreatedPayload } from './change-stream/message-payload.schema'

// Enviar mensagem
await brokerClient.sendMessage('users.created', {
  userId: '123',
  email: 'user@example.com',
  name: 'John Doe',
  createdAt: new Date()
} as UserCreatedPayload)

// Consumir mensagens
const consumer = await brokerClient.getConsumer('notification-service', 'users.created', [0, 1])
await consumer.subscribe({
  handler: async (record) => {
    const userData = record.message.value as UserCreatedPayload
    console.log('Novo usuário:', userData)
  }
})
\`\`\`

## Estrutura recomendada:

- Defina interfaces TypeScript para cada tipo de mensagem
- Mantenha o mapeamento topic→payload atualizado
- Use grupos de consumers diferentes para cada serviço
`
		await fs.writeFile(path.join(changeStreamDir, 'README.md'), readmeContent)

		console.log('✅ Change Stream Broker configuration initialized!')
		console.log('📁 Files created:')
		console.log('   - change-stream/config.ts')
		console.log('   - change-stream/message-payload.schema.ts (OBRIGATÓRIO)')
		console.log('   - change-stream/.gitignore')
		console.log('   - change-stream/README.md')
		console.log('')
		console.log('📝 Next steps:')
		console.log('   1. Review and customize the generated files')
		console.log('   2. Run: npx csbroker generate')
		console.log('   3. Use the generated client in your application')
		console.log('')
		console.log(
			'⚠️  OBS: message-payload.schema.ts é obrigatório para type safety!',
		)
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
