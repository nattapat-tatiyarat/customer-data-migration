import { MongoClient } from "mongodb";
import { readFile } from "fs";

export const updateOpenAccountAtbyCreatedAt = (mongo_db, mongo_collection, mongo_uri, path) => {
    MongoClient.connect(mongo_uri, { useNewUrlParser: true }, (err, client) => {
        if (err) throw err;
        console.log("Connected successfully to server");

        const collection = client.db(mongo_db).collection(mongo_collection);

        readFile(path, "utf8", async (err, data) => {
            if (err) throw err;
            const records = data.split("\n").map((line) => line.split(","));
            const recordsWithoutHeader = records.slice(1);
            let updatedDocuments = 0;
            let updateArray = [];

            for (let row of recordsWithoutHeader) {
                if (row.length < 3) {
                    console.warn(`Skipping incomplete row: ${row.join(',')}`);
                    continue;
                }
                let user_id = parseInt(row[1]) || 0;
                let createdAtString = row[2]
                let createdAtDate = new Date(createdAtString)
                let updated_at = new Date();

                updateArray.push({
                    updateOne: {
                        filter: {
                            user_id: user_id,
                        },
                        update: {
                            $set: {
                                "account.open_account_at": createdAtDate,
                                updated_at: updated_at
                            }
                        },
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
                        console.log(`Chunk ${i + 1}/${n} complete. Matched: ${res.matchedCount}, Modified: ${res.modifiedCount}. Total modified so far: ${updatedDocuments}`);
                    })
                    .catch((err) => {
                        console.error(`Error processing chunk ${i + 1}/${n}:`, err);
                    });
            }
            console.log(`Migration finished. Total documents modified: ${updatedDocuments}`);
            client.close();
        });
    });
};
