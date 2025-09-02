export class BackoffManager {
	private attempt = 0

	constructor(
		private maxRetries: number = 10,
		private baseDelay: number = 1000,
		private maxDelay: number = 30000,
		private jitter: boolean = true, // ← Adicionar jitter
	) {}

	getNextDelay(): number {
		const exponentialDelay = this.baseDelay * 2 ** this.attempt
		const delay = Math.min(this.maxDelay, exponentialDelay)

		// Adicionar jitter para evitar sincronização
		const finalDelay = this.jitter
			? delay * (0.8 + 0.4 * Math.random()) // Jitter entre 80-120%
			: delay

		this.attempt++
		return Math.max(100, finalDelay) // Mínimo 100ms
	}

	shouldRetry(): boolean {
		return this.attempt < this.maxRetries
	}

	reset(): void {
		this.attempt = 0
	}

	getAttempt(): number {
		return this.attempt
	}

	// Novo método para reset condicional
	conditionalReset(success: boolean): void {
		if (success) {
			this.reset()
		}
	}
}
