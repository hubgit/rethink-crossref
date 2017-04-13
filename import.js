const r = require('rethinkdbdash')()
const collection = require('rest-collection-stream')

async function setup () {
  await r.dbDrop('crossref').run()

  await r.dbCreate('crossref').run()

  await r.db('crossref').tableCreate('works', { primaryKey: 'DOI' }).run()
}

setup().then(() => {
  const tableStream = r.db('crossref').table('works').toStream({ writable: true })

  const dataStream = collection('https://api.crossref.org/works', {
    qs: { cursor: '*', rows: 1000 },
    data: (res, body) => body.message.items,
    next: (res, body) => {
      const cursor = body.message['next-cursor']
      return cursor ? { cursor } : null
    }
  })

  dataStream.pipe(tableStream).on('finish', () => {
    console.log('finished!')
    r.getPool().drain()
  })
})

