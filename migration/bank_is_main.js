import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

function statusToInt(status) {
  switch (status) {
    case 'PENDING':
      return 0
    case 'APPROVED':
      return 1
    case 'DELETED':
      return 2
    case 'REJECTED':
      return 4
    default:
      return 0
  }
}

function accountTypeToInt(account_type) {
  switch (account_type) {
    case 'SAVINGS':
      return 1
    case 'CURRENT':
      return 2
    default:
      return 0
  }
}

export const bankIsMain = (mongo_db, mongo_collection, mongo_uri, path) => {
  MongoClient.connect(mongo_uri, { useNewUrlParser: true }, (err, client) => {
    if (err) throw err
    console.log('Connected successfully to server')

    const collection = client.db(mongo_db).collection(mongo_collection)

    readFile(path, 'utf8', async (err, data) => {
      if (err) throw err
      const records = data.split('\n').map((line) => line.split(','))
      const recordsWithoutHeader = records.slice(1)
      let updatedDocuments = 0
      let updateArray = []

      for (let row of recordsWithoutHeader) {
        let user_id = parseInt(row[0]) || 0
        let doc = {
          sub_main_bank_acc_id: parseInt(row[1]) || 0,
          red_main_bank_acc_id: parseInt(row[1]) || 0,
        }
        let doc2 = {
          bank_code: row[10].replaceAll('"', '') || '',
          bank_account_id: parseInt(row[1]) || 0,
          is_main: Boolean(row[2]) || 0,
          bank_name: row[3] || '',
          bank_branch_no: row[4].replaceAll('"', '') || '',
          bank_branch: row[5] || '',
          bank_account_name: row[7] || '',
          bank_account_no: row[8].replaceAll('"', '') || '',
          bank_account_type: accountTypeToInt(row[6]),
          bank_ats_channel: row[9].replaceAll('"', '') || '',
          status: statusToInt(row[11]),
          ref_bank_account_id: parseInt(row[1]) || 0,
        }

        if (row[11] == 'APPROVED') {
          doc2['approved_at'] = row[12].replaceAll('"', '') || ''
        }

        let updated_at = new Date()
        updateArray.push({
          updateOne: {
            filter: {
              user_id: user_id,
            },
            update: {
              $set: {
                'account.sub_main_bank_acc_id': doc['sub_main_bank_acc_id'],
                'account.red_main_bank_acc_id': doc['red_main_bank_acc_id'],
                bank_account: [doc2],
                updated_at: updated_at,
              },
            },
          },
        })
      }

      const startTime = new Date()
      let counter = 0

      let chunkSize = 20000
      let totalRows = updateArray.length
      let n = Math.ceil(totalRows / chunkSize)
      for (let i = 0; i < n; i++) {
        let start = i * chunkSize
        let end = (i + 1) * chunkSize
        if (end > totalRows) {
          end = totalRows
        }
        await collection
          .bulkWrite(updateArray.slice(start, end))
          .then((res) => {
            updatedDocuments += res.matchedCount
            updateArray.slice(start, end).forEach((update) => {
              counter += 1
              console.log(
                `Updated user_id: ${update.updateOne.filter.user_id}, Total updated: ${counter}`
              )
            })
          })
          .catch((err) => {
            console.error(err)
          })
      }

      const endTime = new Date()
      const timeSpentMs = endTime - startTime
      const timeSpentMinutes = timeSpentMs / (1000 * 60) // Convert milliseconds to minutes
      console.log(
        `Time spent: ${timeSpentMinutes.toFixed(
          2
        )} minutes ,Total matched: ${updatedDocuments}`
      )

      client.close()
    })
  })
}
