import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const roboAdvisor = (mongo_db, mongo_collection, mongo_uri, path) => {
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
        let license_no = row[0].replaceAll('"', '') || ''
        let doc = {
          type: row[1].replaceAll('"', '') || '',
          display_name_th: row[2].replaceAll('"', '') || '',
          email: row[3].replaceAll('"', '') || '',
        }
        let updated_at = new Date()
        updateArray.push({
          updateMany: {
            filter: {
              'advisor.license_no': license_no,
            },
            update: {
              $set: {
                'advisor.detailed_type': 'Robo',
                'advisor.name': doc['display_name_th'],
                'advisor.email': doc['email'],
                'advisor.tier': doc['type'],
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
                `Updated advisor.license_no: ${update.updateMany.filter['advisor.license_no']}, Total updated: ${counter}`
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
