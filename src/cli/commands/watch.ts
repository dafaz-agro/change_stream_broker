import path from 'node:path'
import chokidar from 'chokidar'
import { generateClient } from './generate'

export async function watchForChanges(): Promise<void> {
	const configDir = path.join(process.cwd(), 'change-stream')

	console.log(
		'👀 Watching for changes in config.ts and message-payload.schema.ts...',
	)

	chokidar
		.watch(
			[
				path.join(configDir, 'config.ts'),
				path.join(configDir, 'message-payload.schema.ts'),
			],
			{
				persistent: true,
				ignoreInitial: true,
			},
		)
		.on('all', async (event, path) => {
			console.log(`📁 ${event}: ${path.split('/').pop()}`)
			try {
				await generateClient()
			} catch (error) {
				console.error('❌ Regeneration failed:', error)
			}
		})
}
