import { MongoClient } from "mongodb";
import { readFile } from "fs";

export const identity = (mongo_db, mongo_collection, mongo_uri, path) => {
    MongoClient.connect(mongo_uri, { useNewUrlParser: true }, async (err, client) => {
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
                    state: parseInt(recordsWithoutHeader[i][1]) || 0,
                    identity_type: recordsWithoutHeader[i][2] || "",
                    source_type: recordsWithoutHeader[i][3] || "",
                    identity_updated_at: new Date(recordsWithoutHeader[i][4]) || new Date(),
                    updated_at: new Date(),
                };
                updateArray.push({
                    updateOne: {
                        filter: {
                            user_id: doc["user_id"],
                        },
                        update: {
                            $set: {
                                "identity.state": doc["state"],
                                "identity.identity_type": doc["identity_type"],
                                "identity.source_type": doc["source_type"],
                                "identity.updated_at": doc["identity_updated_at"],
                                updated_at: doc["updated_at"],
                            },
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
                        console.log(`Updated ${updatedDocuments} records`);
                    })
                    .catch((err) => {
                        console.error(err);
                    });
            }

            client.close();
        });
    });
};
