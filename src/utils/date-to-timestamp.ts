import { Timestamp } from 'mongodb'

export const DateToTimestamp = (date: Date): Timestamp => {
	const seconds = Math.floor(date.getTime() / 1000)
	return Timestamp.fromBits(seconds, 0)
}
