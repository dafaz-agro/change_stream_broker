export class BackoffManager {
	private attempt = 0

	constructor(
		private maxRetries: number = 10,
		private baseDelay: number = 1000,
		private maxDelay: number = 30000,
	) {}

	getNextDelay(): number {
		const delay = Math.min(this.maxDelay, this.baseDelay * 2 ** this.attempt)
		this.attempt++
		return delay
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
}
