import { MongoClient } from "mongodb";
import { readFile } from "fs";

export const advisor = (mongo_db, mongo_collection, mongo_uri, path) => {
  MongoClient.connect(
    mongo_uri,
    { useNewUrlParser: true },
    async (err, client) => {
      if (err) throw err;
      console.log("Connected successfully to server");

      const collection = client.db(mongo_db).collection(mongo_collection);

      readFile(path, "utf8", async function (err, data) {
        if (err) throw err;
        const records = data.split("\n").map((line) => line.split(","));
        const recordsWithoutHeader = records.slice(1);
        let updatedDocuments = 0;
        let updateArray = [];
        for (let i = 0; i < recordsWithoutHeader.length; i++) {
          let doc = {
            user_id: parseInt(recordsWithoutHeader[i][0]) || 0,
            partner_platform: recordsWithoutHeader[i][1] || "",
            type: recordsWithoutHeader[i][2] || "",
            detailed_type: recordsWithoutHeader[i][3] || "",
            license_no: recordsWithoutHeader[i][4] || "",
            name: recordsWithoutHeader[i][5] || "",
            email: recordsWithoutHeader[i][6] || "",
            tier: recordsWithoutHeader[i][7] || "",
            befund_user_id: recordsWithoutHeader[i][8] || "",
          };
          let updated_at = new Date();
          updateArray.push({
            updateOne: {
              filter: {
                user_id: doc["user_id"],
              },
              update: [
                {
                  $unset: ["advisor"], // Need unset for case advisor is null
                },
                {
                  $set: {
                    "advisor.partner_platform": doc["partner_platform"],
                    "advisor.type": doc["type"],
                    "advisor.detailed_type": doc["detailed_type"],
                    "advisor.license_no": doc["license_no"],
                    "advisor.name": doc["name"],
                    "advisor.email": doc["email"],
                    "advisor.tier": doc["tier"],
                    "advisor.befund_user_id": Number(doc["befund_user_id"]),
                    updated_at: updated_at,
                  },
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
      });
    }
  );
};
