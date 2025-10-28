import { ensureLoggableError } from '@platformatic/foundation'
import getGlobal, { type PlatformaticGlobal } from '@platformatic/globals'
import {
  Admin,
  kOptions,
  parseBroker,
  type ClientDiagnosticEvent,
  type Consumer,
  type CreationEvent
} from '@platformatic/kafka'
import { randomUUID } from 'node:crypto'
import { subscribe, type ChannelListener } from 'node:diagnostics_channel'

export * from './version.ts'

export interface WattHealthPluginOptions {
  maxAllowedLag?: number
  topics: Record<string, number>
  interval: number
  gracePeriod: number
}

export interface Signal {
  type: 'kafka:topics:lag'
  value: number
  description: string
}
export const defaultOptions: Partial<WattHealthPluginOptions> = {
  maxAllowedLag: 1000,
  interval: 60000,
  gracePeriod: 1000
}

function isPositiveNumber (value: unknown): boolean {
  return typeof value === 'number' && !isNaN(value) && value > 0
}

const platformatic = getGlobal() as PlatformaticGlobal & { sendHealthSignal: (signal: Signal) => void }

class WattHealthPlugin {
  #interval: number
  #maxAllowedLag: number
  #maxAllowedTopicLag: Map<string, number>
  #boundChannelSubscriber: ChannelListener
  #boundOnConsumerLag: ChannelListener
  #admins: Map<string, Admin>
  #activeConsumers: Set<string> = new Set()
  #enabled: boolean = false
  #gracePeriodInterval: NodeJS.Timeout | undefined

  constructor (options: Partial<WattHealthPluginOptions>) {
    /* c8 ignore next 3 - Else branches */
    const gracePeriod = options.gracePeriod ?? defaultOptions.gracePeriod!
    this.#interval = options.interval ?? defaultOptions.interval!
    this.#maxAllowedLag = options.maxAllowedLag ?? defaultOptions.maxAllowedLag!
    this.#admins = new Map()
    this.#activeConsumers = new Set()
    this.#enabled = false

    if (!isPositiveNumber(this.#maxAllowedLag)) {
      throw new Error('The maxAllowedLag option must be a positive number.')
    }

    if (!isPositiveNumber(this.#interval)) {
      throw new Error('The interval option must be a positive number.')
    }

    if (!isPositiveNumber(gracePeriod)) {
      throw new Error('The gracePeriod option must be a positive number.')
    }

    this.#maxAllowedTopicLag = new Map(Object.entries(options.topics ?? []))

    for (const [topic, topicLag] of this.#maxAllowedTopicLag) {
      if (!isPositiveNumber(topicLag)) {
        throw new Error(`The max allowed lag for topic "${topic}" must be a positive number.`)
      }
    }

    // Any time a consumer is created, start monitoring it
    this.#boundChannelSubscriber = this.#onNewConsumer.bind(this) as ChannelListener
    subscribe('plt:kafka:instances', this.#boundChannelSubscriber)

    // Monitor any lag retrieved by consumers
    this.#boundOnConsumerLag = this.#onConsumerLag.bind(this) as ChannelListener
    subscribe('plt:kafka:consumer:lag', this.#boundOnConsumerLag)

    platformatic.events.on('close', () => {
      for (const admin of this.#admins.values()) {
        admin.close()
      }
    })

    this.#gracePeriodInterval = setTimeout(() => {
      this.setEnabled(true)
    }, gracePeriod).unref()
  }

  setEnabled (enabled: boolean) {
    if (this.#gracePeriodInterval) {
      clearTimeout(this.#gracePeriodInterval)
      this.#gracePeriodInterval = undefined
    }

    this.#enabled = enabled
  }

  #onNewConsumer ({ instance, type }: CreationEvent<unknown>) {
    if (type !== 'consumer') {
      return
    }

    const consumer = instance as Consumer
    consumer.startLagMonitoring({ topics: [] }, this.#interval)
  }

  #onConsumerLag ({ client: consumer, lag }: ClientDiagnosticEvent<Consumer, { lag: Map<string, bigint[]> }>) {
    const groupId = consumer.groupId
    const lagsToSignal: Record<string, number> = {}

    // Ignore reports if disabled
    if (!this.#enabled) {
      return
    }

    // Find all topics that are lagging more than allowed
    for (const [topic, lags] of lag) {
      const maxAllowedLag = BigInt(this.#maxAllowedTopicLag.get(topic) ?? this.#maxAllowedLag)
      for (const lag of lags) {
        if (lag > maxAllowedLag) {
          lagsToSignal[topic] = Number(lag)
          break
        }
      }
    }

    const laggingTopics = Object.keys(lagsToSignal)

    // Nothing is lagging or we are already checking the number of consumers, move on
    if (laggingTopics.length === 0 || this.#activeConsumers.has(consumer.memberId!)) {
      return
    }

    this.#activeConsumers.add(consumer.memberId!)

    // Create an admin client sharing the same connection pool as the consumer
    const bootstrapBrokers = consumer[kOptions].bootstrapBrokers.map(parseBroker).join(',')

    // For this consumer, fetch the number of partitions using the metadata.
    // The call is technically async, but given the metadata is usually cached,
    // it will likely not add any delay.
    consumer.metadata({ topics: laggingTopics }, (err, metadata) => {
      /* c8 ignore next 5 - Hard to test */
      if (err) {
        this.#activeConsumers.delete(consumer.memberId!)
        platformatic.logger.error({ err: ensureLoggableError(err) }, 'Error fetching metadata for lagging topics')
        return
      }

      let admin = this.#admins.get(bootstrapBrokers)
      if (!admin) {
        admin = new Admin({
          ...consumer[kOptions],
          clientId: `watt-health-plugin-admin-${randomUUID()}`
        })

        this.#admins.set(bootstrapBrokers, admin)
      }

      // Now fetch the number of members for the consumer group
      admin.describeGroups({ groups: [groupId] }, (err, group) => {
        /* c8 ignore next 5 - Hard to test */
        if (err) {
          platformatic.logger.error({ err: ensureLoggableError(err) }, 'Cannot get members for consumer group')
          return
        }

        this.#activeConsumers.delete(consumer.memberId!)

        for (const topic of laggingTopics) {
          /* c8 ignore next 2 - Else branches */
          const maximumConsumers = metadata.topics.get(topic)?.partitions.length ?? 0
          const currentConsumers = group.get(consumer.groupId)?.members.size ?? 0

          if (currentConsumers < maximumConsumers) {
            platformatic.sendHealthSignal({
              type: 'kafka:topics:lag',
              value: lagsToSignal[topic],
              description: `Member ${consumer.memberId} of consumers group ${groupId} is lagging for topic ${topic} and there are available consumers in the group.`
            })
          }
        }
      })
    })
  }
}

export function create (options: Partial<WattHealthPluginOptions>): WattHealthPlugin {
  return new WattHealthPlugin(options)
}
