import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

const email = (mongo_db, mongo_collection, mongo_uri, path) => {
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
				var updatedDocuments = 0

				for (let i = 0; i < recordsWithoutHeader.length; i++) {
					let doc = {}
					doc['user_id'] = parseInt(recordsWithoutHeader[i][0]) || 0
					doc['email'] = recordsWithoutHeader[i][1] || ''
					doc['last_login_channel'] = recordsWithoutHeader[i][2] || ''
					doc['external_id'] = recordsWithoutHeader[i][3] || ''
					doc['partner_user_id'] = recordsWithoutHeader[i][4] || ''
					doc['updated_at'] = new Date()

					collection.updateOne(
						{
							user_id: doc['user_id']
						},
						{
							$set: {
								email: doc['email'],
								last_login_channel: doc['last_login_channel'],
								external_id: doc['external_id'],
								partner_user_id: doc['partner_user_id'],
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

export default email
