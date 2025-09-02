import { ResumeToken } from 'mongodb'
import { ConsumerGroup, ConsumerMap } from '../types'
import { Logger } from '../utils/logger'
import { ChangeStreamConsumer } from './consumer'

export class ConsumerGroupManager {
	private consumerGroups: Map<string, ConsumerGroup> = new Map()
	private logger: typeof Logger

	constructor() {
		this.logger = Logger.withContext('Consumer Group')
	}

	createConsumerGroup(groupId: string, topics: string[]): ConsumerGroup {
		const group: ConsumerGroup = {
			groupId,
			topics,
			lastOffsets: new Map(),
			members: new Map() as ConsumerMap, // ← Usando o tipo da interface
		}

		this.consumerGroups.set(groupId, group)
		this.logger.info(
			`Consumer group ${groupId} created for topics: ${topics.join(', ')}`,
		)

		return group
	}

	getConsumerGroup(groupId: string): ConsumerGroup | undefined {
		return this.consumerGroups.get(groupId)
	}

	addMemberToGroup(
		groupId: string,
		consumerId: string,
		consumer: ChangeStreamConsumer,
	): void {
		const group = this.consumerGroups.get(groupId)
		if (!group) {
			throw new Error(`Consumer group ${groupId} not found`)
		}

		group.members.set(consumerId, consumer)
		this.logger.info(`Consumer ${consumerId} added to group ${groupId}`)
	}

	removeMemberFromGroup(groupId: string, consumerId: string): void {
		const group = this.consumerGroups.get(groupId)
		if (group) {
			group.members.delete(consumerId)
			this.logger.info(`Consumer ${consumerId} removed from group ${groupId}`)
		}
	}

	updateOffset(
		groupId: string,
		topic: string,
		offset: ResumeToken,
		partition: number,
	): void {
		const group = this.consumerGroups.get(groupId)
		if (group) {
			const partitionKey = `${topic}_p${partition}`
			group.lastOffsets.set(partitionKey, offset)
		}
	}

	getOffset(
		groupId: string,
		topic: string,
		partition: number,
	): ResumeToken | undefined {
		const group = this.consumerGroups.get(groupId)
		const partitionKey = `${topic}_p${partition}`
		return group?.lastOffsets.get(partitionKey)
	}

	async rebalanceGroup(groupId: string): Promise<void> {
		const group = this.consumerGroups.get(groupId)
		if (!group) return

		this.logger.info(`Rebalancing consumer group ${groupId}`)

		for (const [consumerId, _] of group.members) {
			this.logger.info(`Consumer ${consumerId} in group ${groupId} rebalanced`)
		}
	}

	async disconnectGroup(groupId: string): Promise<void> {
		const group = this.consumerGroups.get(groupId)
		if (!group) return

		this.logger.info(`Disconnecting all consumers in group ${groupId}`)

		for (const [_, consumer] of group.members) {
			await consumer.disconnect()
		}

		group.members.clear()
		this.consumerGroups.delete(groupId)
	}

	getGroupMembers(groupId: string): ConsumerMap | undefined {
		return this.consumerGroups.get(groupId)?.members
	}

	getAllConsumerGroups(): Map<string, ConsumerGroup> {
		return this.consumerGroups
	}
}
