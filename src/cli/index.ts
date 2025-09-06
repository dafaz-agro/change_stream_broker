#!/usr/bin/env node

import { Command } from 'commander'
import { generateClient } from './commands/generate'
import { initConfiguration } from './commands/init'

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

program.parse()
