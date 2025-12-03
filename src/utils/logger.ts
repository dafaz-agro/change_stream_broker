import { LogLevel } from '@/types'

export interface ILogger {
	debug(message: string, ...args: unknown[]): void
	info(message: string, ...args: unknown[]): void
	warn(message: string, ...args: unknown[]): void
	error(message: string, ...args: unknown[]): void
}

export class Logger implements ILogger {
	private static level: LogLevel = LogLevel.INFO
	private static isEnabled: boolean = true

	private readonly context: string = ''

	constructor(context: string = '') {
		this.context = context
	}

	// Configuração global do logger
	static configure(config: { level?: LogLevel; enabled?: boolean }): void {
		if (config.level !== undefined) {
			Logger.level = config.level
		}
		if (config.enabled !== undefined) {
			Logger.isEnabled = config.enabled
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

	static getCurrentLevel(): LogLevel {
		return Logger.level
	}

	static isLogEnabled(): boolean {
		return Logger.isEnabled
	}

	// Método para logs críticos (sempre loga, mesmo se desativado)
	static critical(message: string, ...args: unknown[]): void {
		console.error(`[CRITICAL] ${new Date().toISOString()}: ${message}`, ...args)
	}

	// Criar logger com contexto específico
	static withContext(context: string): ILogger {
		return new Logger(context)
	}

	// Verificar se deve logar
	private shouldLog(level: LogLevel): boolean {
		return Logger.isEnabled && level <= Logger.level
	}

	// Formatar mensagem
	private formatMessage(level: string, message: string): string {
		const timestamp = new Date().toISOString()
		const context = this.context ? ` [${this.context}]` : ''
		return `[${level}] ${timestamp}${context}: ${message}`
	}

	// Métodos de log com verificação de nível
	debug(message: string, ...args: unknown[]): void {
		if (this.shouldLog(LogLevel.DEBUG)) {
			console.debug(this.formatMessage('DEBUG', message), ...args)
		}
	}

	info(message: string, ...args: unknown[]): void {
		if (this.shouldLog(LogLevel.INFO)) {
			console.log(this.formatMessage('INFO', message), ...args)
		}
	}

	warn(message: string, ...args: unknown[]): void {
		if (this.shouldLog(LogLevel.WARN)) {
			console.warn(this.formatMessage('WARN', message), ...args)
		}
	}

	error(message: string, ...args: unknown[]): void {
		if (this.shouldLog(LogLevel.ERROR)) {
			console.error(this.formatMessage('ERROR', message), ...args)
		}
	}
}
