import { stringSerializers } from '@platformatic/kafka'
import { create } from '@platformatic/runtime'
import { deepStrictEqual, throws } from 'node:assert'
import EventEmitter, { once } from 'node:events'
import { resolve as resolvePaths } from 'node:path'
import { test } from 'node:test'
import { create as createPlugin, type Signal } from '../src/index.ts'
import { createProducer, createTopic } from './helpers.ts'

export declare class Runtime extends EventEmitter {
  start (): Promise<string>
  close (): Promise<void>
}

test('constructor should throw error for invalid values', () => {
  for (const value of [-1, 0, NaN, 'invalid']) {
    // @ts-expect-error Testing invalid values
    throws(() => createPlugin({ maxAllowedLag: value }), /The maxAllowedLag option must be a positive number\./)

    // @ts-expect-error Testing invalid values
    throws(() => createPlugin({ interval: value }), /The interval option must be a positive number\./)

    // @ts-expect-error Testing invalid values
    throws(() => createPlugin({ gracePeriod: value }), /The gracePeriod option must be a positive number\./)

    throws(
      // @ts-expect-error Testing invalid values
      () => createPlugin({ topics: { test: value } }),
      /The max allowed lag for topic "test" must be a positive number\./
    )
  }
})

test('should issue a signal when the consumer is lagging', async t => {
  const producer = createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  // Produce 5 messages
  for (let i = 0; i < 15; i++) {
    await producer.send({ messages: [{ topic, key: `key-${i}`, value: `message-${i}`, partition: i % 3 }] })
  }

  const runtime = (await create(resolvePaths(import.meta.dirname, './fixtures/single/watt.json'))) as Runtime

  const url = await runtime.start()
  t.after(() => {
    return runtime.close()
  })

  await fetch(`${url}/consume/${topic}`)

  const { promise, resolve } = Promise.withResolvers<Signal[]>()

  function waitForSignals (payload: any) {
    if (payload.healthSignals.length > 0) {
      resolve(payload.healthSignals)
      runtime.removeListener('application:worker:health:metrics', waitForSignals)
    }

    payload.healthSignals = []
  }

  runtime.on('application:worker:health:metrics', waitForSignals)

  const signals = await promise
  deepStrictEqual(signals[0].type, 'kafka:topics:lag')
  deepStrictEqual(signals[0].value, 4)
  deepStrictEqual(signals[0].level, 'critical')
})

test('should issue no signals when the consumer is lagging but there are as many consumer as partitions', async t => {
  const producer = createProducer(t, { serializers: stringSerializers })
  const topic = await createTopic(t, true)

  // Produce 5 messages
  for (let i = 0; i < 15; i++) {
    await producer.send({ messages: [{ topic, key: `key-${i}`, value: `message-${i}`, partition: i % 3 }] })
  }

  const runtime = (await create(resolvePaths(import.meta.dirname, './fixtures/multiple/watt.json'))) as Runtime

  const url = await runtime.start()
  t.after(() => {
    return runtime.close()
  })

  await fetch(`${url}/consume/${topic}`)

  let healthSignals = 0

  runtime.on('application:worker:health:metrics', (payload: any) => {
    healthSignals += payload.healthSignals.length
  })

  await once(runtime, 'application:worker:event:done')
  deepStrictEqual(healthSignals, 0)
})
