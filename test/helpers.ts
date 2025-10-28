import { Admin, Consumer, Producer, type ConsumerOptions, type ProducerOptions } from '@platformatic/kafka'
import { randomUUID } from 'crypto'
import { type TestContext } from 'node:test'

const bootstrapBrokers = ['localhost:9001']

export function createConsumer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ConsumerOptions<Key, Value, HeaderKey, HeaderValue>> = {}
) {
  const options: ConsumerOptions<Key, Value, HeaderKey, HeaderValue> = {
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId: `test-consumer-${randomUUID()}`,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...overrideOptions
  }

  const consumer = new Consumer<Key, Value, HeaderKey, HeaderValue>(options)
  t.after(() => consumer.close(true))

  return consumer
}

export function createProducer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ProducerOptions<Key, Value, HeaderKey, HeaderValue>> = {}
) {
  const options: ProducerOptions<Key, Value, HeaderKey, HeaderValue> = {
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers,
    ...overrideOptions
  }

  const producer = new Producer<Key, Value, HeaderKey, HeaderValue>(options)
  t.after(() => producer.close())

  return producer
}

export async function createTopic (t: TestContext, create: boolean = false) {
  const topic = `test-topic-${randomUUID()}`

  if (create) {
    const admin = new Admin({ clientId: `test-admin-${randomUUID()}`, bootstrapBrokers })
    t.after(() => admin.close())

    await admin.createTopics({ topics: [topic], partitions: 3 })
  }

  return topic
}
