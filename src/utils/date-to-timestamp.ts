import { Timestamp } from 'mongodb'

export const DateToTimestamp = (date: Date): Timestamp => {
	return Timestamp.fromBits(Math.floor(date.getTime() / 1000), 0)
}
