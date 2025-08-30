import { ResumeToken } from 'mongodb'
import {
	ConsumerGroup,
	ConsumerMap,
	IChangeStreamConsumer,
} from '../types/types'
import { Logger } from '../utils/logger'

export class ConsumerGroupManager {
	private consumerGroups: Map<string, ConsumerGroup> = new Map()

	createConsumerGroup(groupId: string, topics: string[]): ConsumerGroup {
		const group: ConsumerGroup = {
			groupId,
			topics,
			lastOffsets: new Map(),
			members: new Map() as ConsumerMap, // ← Usando o tipo da interface
		}

		this.consumerGroups.set(groupId, group)
		Logger.info(
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
		consumer: IChangeStreamConsumer,
	): void {
		const group = this.consumerGroups.get(groupId)
		if (!group) {
			throw new Error(`Consumer group ${groupId} not found`)
		}

		group.members.set(consumerId, consumer)
		Logger.info(`Consumer ${consumerId} added to group ${groupId}`)
	}

	removeMemberFromGroup(groupId: string, consumerId: string): void {
		const group = this.consumerGroups.get(groupId)
		if (group) {
			group.members.delete(consumerId)
			Logger.info(`Consumer ${consumerId} removed from group ${groupId}`)
		}
	}

	updateOffset(groupId: string, topic: string, offset: ResumeToken): void {
		const group = this.consumerGroups.get(groupId)
		if (group) {
			group.lastOffsets.set(topic, offset)
		}
	}

	getOffset(groupId: string, topic: string): ResumeToken | undefined {
		const group = this.consumerGroups.get(groupId)
		return group?.lastOffsets.get(topic)
	}

	async rebalanceGroup(groupId: string): Promise<void> {
		const group = this.consumerGroups.get(groupId)
		if (!group) return

		Logger.info(`Rebalancing consumer group ${groupId}`)

		for (const [consumerId, _] of group.members) {
			Logger.info(`Consumer ${consumerId} in group ${groupId} rebalanced`)
		}
	}

	async disconnectGroup(groupId: string): Promise<void> {
		const group = this.consumerGroups.get(groupId)
		if (!group) return

		Logger.info(`Disconnecting all consumers in group ${groupId}`)

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
