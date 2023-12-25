import { MongoClient } from "mongodb";

export const remove_subscriber = (mongo_db, mongo_collection, mongo_uri) => {
  MongoClient.connect(
    mongo_uri,
    { useNewUrlParser: true },
    async (err, client) => {
      if (err) throw err;
      console.log("Connected successfully to server");
      const collection = client.db(mongo_db).collection(mongo_collection);
      let data = await collection.find({ wt_status: { $lte: 2 } }).toArray();
      let updatedDocuments = 0;
      let updateArray = [];
      for (let i = 0; i < data.length; i++) {
        let user_id = data[i].user_id || 0;
        let updated_at = new Date();
        updateArray.push({
          updateOne: {
            filter: {
              user_id: user_id,
              wt_status: { $lte: 2 },
            },
            update: [
              { $set: { updated_at: updated_at } },
              {
                $unset: [
                  "befund_customer_id",
                  "customer_type",
                  "agent_type",
                  "name",
                  "surname",
                  "segment",
                  "segment_expired_date",
                  "total_aua",
                  "total_cost",
                  "personal_info",
                  "account",
                  "advisor",
                  "wt_status",
                ],
              },
            ],
          },
        });
      }
      let chunkSize = 20000;
      let totalRows = updateArray.length;
      let n = Math.ceil(totalRows / chunkSize);
      for (let i = 0; i < n; i++) {
        let start = i * chunkSize;
        let end = (i + 1) * chunkSize;
        if (end > totalRows) {
          end = totalRows;
        }
        await collection
          .bulkWrite(updateArray.slice(start, end))
          .then((res) => {
            updatedDocuments += res.matchedCount;
            console.log(`Updated ${updatedDocuments} records`);
          })
          .catch((err) => {
            console.error(err);
          });
      }
      client.close();
    }
  );
};
