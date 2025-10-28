import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes } from '@platformatic/kafka'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { create } from '../../../src/index.ts'

let consumer

globalThis.platformatic.events.on('close', () => {
  consumer.close(true)
  server.close()
})

const server = createServer(async (req, res) => {
  const topic = req.url.split('/').pop()

  await create({ maxAllowedLag: 3, interval: 1000 })

  consumer = new Consumer({
    clientId: 'test-consumer',
    bootstrapBrokers: ['localhost:9001'],
    groupId: `test-group-${randomUUID()}`
  })

  // Create a stream but pause it after the first message. This creates a 4n lag on a partitin
  const stream = await consumer.consume({
    topics: [topic],
    maxBytes: 100,
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST
  })

  stream.once('data', data => {
    stream.pause()
  })

  res.writeHead(200).end('OK')
})

server.listen(0)
