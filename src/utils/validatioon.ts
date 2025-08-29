import { Document, OptionalId } from 'mongodb'
import { Message } from '../broker/types'

export class ValidationUtils {
	static isValidDocument(doc: Document): doc is OptionalId<Document> {
		return (
			doc !== undefined &&
			doc !== null &&
			typeof doc === 'object' &&
			!Array.isArray(doc) &&
			Object.keys(doc).length > 0
		)
	}

	static sanitizeDocument(doc: Document): Document {
		const sanitized: Document = { ...doc }

		Object.keys(sanitized).forEach((key) => {
			if (sanitized[key] === undefined) {
				delete sanitized[key]
			}
		})

		return sanitized
	}

	static createSafeDocument(message: Message): OptionalId<Document> | null {
		try {
			const doc: Document = {}

			// Handle different value types
			if (message.value !== undefined && message.value !== null) {
				if (
					typeof message.value === 'object' &&
					!Array.isArray(message.value)
				) {
					Object.assign(doc, message.value)
				} else {
					doc.value = message.value
				}
			}

			// Add metadata
			if (message.headers) doc._headers = message.headers
			doc._timestamp = message.timestamp || new Date()
			if (message.key) doc._key = message.key

			// Sanitize
			const sanitized = ValidationUtils.sanitizeDocument(doc)

			return ValidationUtils.isValidDocument(sanitized) ? sanitized : null
		} catch (error) {
			return new Error(`Failed to create document from message: ${error}`)
		}
	}
}
