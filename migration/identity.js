import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const identity = (mongo_db, mongo_collection, mongo_uri, path) => {
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
            state: parseInt(recordsWithoutHeader[i][1]) || 0,
            identity_type: recordsWithoutHeader[i][2] || '',
            source_type: recordsWithoutHeader[i][3] || '',
            identity_updated_at: new Date(recordsWithoutHeader[i][4]) || new Date(),
            updated_at: new Date()
          }

          collection.updateOne(
            {
              user_id: doc['user_id']
            },
            {
              $set: {
                "identity.state": doc['state'],
                "identity.identity_type": doc['identity_type'],
                "identity.source_type": doc['source_type'],
                "identity.updated_at": doc['identity_updated_at'],
                updated_at: doc['updated_at']
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
