import { MongoClient } from "mongodb";
import { readFile } from "fs";

export const plans = (mongo_db, mongo_collection, mongo_uri, path) => {
    MongoClient.connect(mongo_uri, { useNewUrlParser: true }, (err, client) => {
        if (err) throw err;
        console.log("Connected successfully to server");

        const collection = client.db(mongo_db).collection(mongo_collection);

        readFile(path, "utf8", async (err, data) => {
            if (err) throw err;
            const records = data.split("\n").map((line) => line.split(","));
            const rows = records.slice(1);
            let updateArray = [];
            let updatedDocuments = 0;
            let user_id = null;
            let user_data = [];

            for (let i = 0; i < rows.length; i++) {
                let doc = {
                        agent_account_id: rows[i][1] !== "NULL" ? rows[i][1] : null,
                        account_type: rows[i][2] || "",
                        plan_slot: parseInt(rows[i][3]) || 0,
                        plan_id: parseInt(rows[i][4]) || 0,
                        created_date: rows[i][5].split(' ')[0],
                        plan_type: rows[i][6] || "",
                        goal_type: rows[i][7] || "",
                        plan_name: rows[i][8] || "",
                        risk_level: parseInt(rows[i][9]) || 0,
                        pra_status: rows[i][10] !== "NULL" ? rows[i][10] : null,
                        order_no: parseInt(rows[i][12]) || 0,
                        is_hide: rows[i][13] == 1 ? true : false,
                };
                if (user_id === null) {
                    user_id =  parseInt(rows[i][0]);
                    user_data.push(doc)
                } else if (user_id === parseInt(rows[i][0])){
                    user_data.push(doc)
                } else {
                    console.log(`process user_id: ${user_id}`); // Output the accumulated data
                    updateArray.push({
                    updateOne: {
                        filter: {
                            user_id: user_id,
                        },
                        update: {
                            $set: { updated_at:  new Date(), port: user_data },
                        },
                    },
                });
                    user_id = parseInt(rows[i][0]);
                    user_data = [doc]; // Reset the array with new user ID data
                }
            }

            // Output the remaining data (last user)
            if (user_data.length > 0) {
                console.log(`last user_id: ${user_id}`);
                updateArray.push({
                    updateOne: {
                        filter: {
                            user_id: user_id,
                        },
                        update: {
                            $set: { updated_at:  new Date(), port: user_data },
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
