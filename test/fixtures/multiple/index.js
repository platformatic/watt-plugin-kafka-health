import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from '@platformatic/kafka'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { create } from '../../../src/index.ts'

const consumers = []

globalThis.platformatic.events.on('close', () => {
  for (const consumer of consumers) {
    consumer.close(true)
  }

  server.close()
})

const server = createServer(async (req, res) => {
  const topic = req.url.split('/').pop()
  process.env.TOPIC = topic

  const plugin = await create({ maxAllowedLag: 3, interval: 1000 })
  plugin.setEnabled(false)

  let consumed = 0

  const groupId = `test-group-${randomUUID()}`
  for (let i = 0; i < 3; i++) {
    const consumer = new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:9001'],
      groupId
    })

    // Create a stream but pause it after the first message. This creates a 4n lag on a partitin
    const stream = await consumer.consume({
      topics: [topic],
      maxBytes: 100,
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST
    })
    consumers.push(consumer)

    stream.once('data', data => {
      stream.pause()

      consumed++

      // All stream have consumed one message, they should also lag now. Signal the runtime we're done after 5 seconds
      // which should be enough for the plugin to detect the lag and not send any signal
      if (consumed === 3) {
        setTimeout(() => {
          globalThis.platformatic.events.emitAndNotify('done')
        }, 5000)
      }
    })
  }

  plugin.setEnabled(true)
  res.writeHead(200).end('OK')
})

server.listen(0)
