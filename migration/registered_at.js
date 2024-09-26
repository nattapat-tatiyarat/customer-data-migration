import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const registeredAt = (mongo_db, mongo_collection, mongo_uri, path) => {
  MongoClient.connect(mongo_uri, { useNewUrlParser: true }, (err, client) => {
    if (err) throw err
    console.log('Connected successfully to server')

    const collection = client.db(mongo_db).collection(mongo_collection)

    readFile(path, 'utf8', async (err, data) => {
      if (err) throw err
      const records = data.split('\n').map((line) => line.split(','))
      const recordsWithoutHeader = records.slice(1)
      let updatedDocuments = 0
      let updateArray = []

      for (let row of recordsWithoutHeader) {
        let user_id = parseInt(row[0]) || 0
        let doc = {
          registered_at: new Date(row[1])
        }
        updateArray.push({
          updateOne: {
            filter: {
              user_id: user_id
            },
            update: {
              $set: doc
            }
          }
        })
      }

      let chunkSize = 20000
      let totalRows = updateArray.length
      let n = Math.ceil(totalRows / chunkSize)
      for (let i = 0; i < n; i++) {
        let start = i * chunkSize
        let end = (i + 1) * chunkSize
        if (end > totalRows) {
          end = totalRows
        }
        await collection
          .bulkWrite(updateArray.slice(start, end))
          .then((res) => {
            updatedDocuments += res.matchedCount
            console.log(`Updated ${updatedDocuments} records`)
          })
          .catch((err) => {
            console.error(err)
          })
      }

      client.close()
    })
  })
}
