import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const removeExternalUSerIDDup = (
  mongo_db,
  mongo_collection,
  mongo_uri,
  path
) => {
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
        let user_id = parseInt(row[1]) || 0

        let doc = {
          external_user_id: row[0] || '',
        }
        let updated_at = new Date()
        updateArray.push({
          updateOne: {
            filter: {
              user_id: user_id,
            },
            update: {
              $set: {
                external_user_id: doc['external_user_id'],
                updated_at: updated_at,
              },
            },
          },
        })
      }

      const startTime = new Date()
      let counter = 0

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
            updateArray.slice(start, end).forEach((update) => {
              counter += 1
              console.log(
                `Updated user_id: ${update.updateOne.filter.user_id}, Total updated: ${counter}`
              )
            })
          })
          .catch((err) => {
            console.error(err)
          })
      }

      const endTime = new Date()
      const timeSpentMs = endTime - startTime
      const timeSpentMinutes = timeSpentMs / (1000 * 60) // Convert milliseconds to minutes
      console.log(
        `Time spent: ${timeSpentMinutes.toFixed(
          2
        )} minutes ,Total matched: ${updatedDocuments}`
      )

      client.close()
    })
  })
}
