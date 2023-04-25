import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const port = (mongo_db, mongo_collection, mongo_uri, path) => {
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
          agent_account_id: row[1] !== 'NULL' ? row[1] : null,
          account_type: row[2] || '',
          plan_slot: parseInt(row[3]) || 0,
          plan_id: parseInt(row[4]) || 0,
          created_date: row[5] || '',
          plan_type: row[6] || '',
          goal_type: row[7] || '',
          plan_name: row[8] || '',
          risk_level: parseInt(row[9]) || 0,
          pra_status: row[10] || '',
          order_no: parseInt(row[12]) || 0,
          is_hide: parseInt(row[13]) || 0
        }
        let updated_at = new Date()
        updateArray.push({
          updateOne: {
            filter: {
              user_id: user_id,
              'port.plan_id': { $ne: doc['plan_id'] }
            },
            update: {
              $set: { updated_at: updated_at },
              $addToSet: { port: doc }
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
