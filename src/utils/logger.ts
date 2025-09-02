import { LogLevel } from '@/types'

export class Logger {
	private static level: LogLevel = LogLevel.INFO
	private static isEnabled: boolean = true
	private static context: string = ''

	// Configuração global do logger
	static configure(config: {
		level?: LogLevel
		enabled?: boolean
		context?: string
	}): void {
		if (config.level !== undefined) {
			Logger.level = config.level
		}
		if (config.enabled !== undefined) {
			Logger.isEnabled = config.enabled
		}
		if (config.context !== undefined) {
			Logger.context = config.context
		}
	}

	// Ativar/desativar logs
	static enable(): void {
		Logger.isEnabled = true
	}

	static disable(): void {
		Logger.isEnabled = false
	}

	// Definir nível de log
	static setLevel(level: LogLevel): void {
		Logger.level = level
	}

	// Métodos de log com verificação de nível
	static debug(message: string, ...args: unknown[]): void {
		if (Logger.shouldLog(LogLevel.DEBUG)) {
			console.debug(Logger.formatMessage('DEBUG', message), ...args)
		}
	}

	static info(message: string, ...args: unknown[]): void {
		if (Logger.shouldLog(LogLevel.INFO)) {
			console.log(Logger.formatMessage('INFO', message), ...args)
		}
	}

	static warn(message: string, ...args: unknown[]): void {
		if (Logger.shouldLog(LogLevel.WARN)) {
			console.warn(Logger.formatMessage('WARN', message), ...args)
		}
	}

	static error(message: string, ...args: unknown[]): void {
		if (Logger.shouldLog(LogLevel.ERROR)) {
			console.error(Logger.formatMessage('ERROR', message), ...args)
		}
	}

	// Método para logs críticos (sempre loga, mesmo se desativado)
	static critical(message: string, ...args: unknown[]): void {
		console.error(Logger.formatMessage('CRITICAL', message), ...args)
	}

	// Verificar se deve logar
	private static shouldLog(level: LogLevel): boolean {
		return Logger.isEnabled && level <= Logger.level
	}

	// Formatar mensagem
	private static formatMessage(level: string, message: string): string {
		const timestamp = new Date().toISOString()
		const context = Logger.context ? ` [${Logger.context}]` : ''
		return `[${level}] ${timestamp}${context}: ${message}`
	}

	// Métodos utilitários
	static getCurrentLevel(): LogLevel {
		return Logger.level
	}

	static isLogEnabled(): boolean {
		return Logger.isEnabled
	}

	// Criar logger com contexto específico
	static withContext(context: string): typeof Logger {
		const contextualLogger = class extends Logger {}
		contextualLogger.configure({ context })
		return contextualLogger
	}
}
