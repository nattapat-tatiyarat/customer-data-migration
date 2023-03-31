import { MongoClient } from 'mongodb'

const deletePartners = (mongo_db, mongo_collection, mongo_uri) => {
	MongoClient.connect(
		mongo_uri,
		{ useNewUrlParser: true },
		async (err, client) => {
			if (err) throw err
			console.log('Connected successfully to server')

			const collection = client.db(mongo_db).collection(mongo_collection)

			collection.updateMany(
				{ partners: { $exists: 1 } },
				{ $unset: { partners: 1 } },
				function (err, result) {
					if (err) throw err
					console.log(`Updated ${result.matchedCount} records`)
					client.close()
				}
			)
		}
	)
}

export default deletePartners
