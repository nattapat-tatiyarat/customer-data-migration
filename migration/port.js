import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const port = (mongo_db, mongo_collection, mongo_uri, path) => {
  MongoClient.connect(
    mongo_uri,
    { useNewUrlParser: true },
    async (err, client) => {
      if (err) throw err
      console.log('Connected successfully to server')

      const collection = client.db(mongo_db).collection(mongo_collection)

      readFile(path, 'utf8', async function (err, data) {
        if (err) throw err
        const records = data.split('\n').map((line) => line.split(','))
        const recordsWithoutHeader = records.slice(1)
        let updatedDocuments = 0

        for (let i = 0; i < recordsWithoutHeader.length; i++) {
          let doc = {
            user_id: parseInt(recordsWithoutHeader[i][0]) || 0,
            agent_account_id: recordsWithoutHeader[i][1] || '',
            account_type: recordsWithoutHeader[i][2] || '',
            plan_slot: parseInt(recordsWithoutHeader[i][3]) || 0,
            plan_id: parseInt(recordsWithoutHeader[i][4]) || 0,
            created_date: recordsWithoutHeader[i][5] || '',
            plan_type: recordsWithoutHeader[i][6] || '',
            goal_type: recordsWithoutHeader[i][7] || '',
            plan_name: recordsWithoutHeader[i][8] || '',
            risk_level: parseInt(recordsWithoutHeader[i][9]) || 0,
            pra_status: recordsWithoutHeader[i][10] || '',
            asset_sorting_id: parseInt(recordsWithoutHeader[i][11]) || 0,
            order_no: parseInt(recordsWithoutHeader[i][12]) || 0,
            is_hide: parseInt(recordsWithoutHeader[i][13]) || 0,
            updated_at: new Date()
          }

          collection.updateOne(
            {
              user_id: doc['user_id'],
              'port.plan_id': { $ne: doc['plan_id'] }
            },
            {
              $set: {
                updated_at: doc['updated_at']
              },
              $addToSet: {
                port: {
                  account_type: doc['account_type'],
                  plan_slot: doc['plan_slot'],
                  plan_id: doc['plan_id'],
                  created_date: doc['created_date'],
                  plan_type: doc['plan_type'],
                  goal_type: doc['goal_type'],
                  plan_name: doc['plan_name'],
                  risk_level: doc['risk_level'],
                  order_no: doc['order_no'],
                  is_hide: doc['is_hide'],
                  agent_account_id: doc['agent_account_id'],
                  pra_status: doc['pra_status']
                }
              }
            },
            function (err, result) {
              if (err) throw err
              updatedDocuments += result.matchedCount
              console.log(`Updated ${updatedDocuments} records`)
              if (i == recordsWithoutHeader.length - 1) {
                client.close()
              }
            }
          )
        }
      })
    }
  )
}
