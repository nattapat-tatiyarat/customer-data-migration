import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

const port = (mongo_db, mongo_collection, mongo_uri, path) => {
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
					doc['agent_account_id'] = recordsWithoutHeader[i][1] || ''
					doc['account_type'] = recordsWithoutHeader[i][2] || ''
					doc['plan_slot'] = recordsWithoutHeader[i][3] || ''
					doc['plan_id'] = parseInt(recordsWithoutHeader[i][4]) || 0
					doc['created_date'] = recordsWithoutHeader[i][5] || ''
					doc['plan_type'] = recordsWithoutHeader[i][6] || ''
					doc['goal_type'] = recordsWithoutHeader[i][7] || ''
					doc['plan_name'] = recordsWithoutHeader[i][8] || ''
					doc['risk_level'] = recordsWithoutHeader[i][9] || ''
					doc['pra_status'] = recordsWithoutHeader[i][10] || ''
					doc['asset_sorting_id'] =
						parseInt(recordsWithoutHeader[i][11]) || 0
					doc['order_no'] = recordsWithoutHeader[i][12] || ''
					doc['is_hide'] = recordsWithoutHeader[i][13] || ''
					doc['updated_at'] = new Date()

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

export default port
