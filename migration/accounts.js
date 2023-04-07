import { MongoClient } from 'mongodb'
import { readFile } from 'fs'

export const accounts = (mongo_db, mongo_collection, mongo_uri, path) => {
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
            customer_code: recordsWithoutHeader[i][1] || '',
            customer_type: recordsWithoutHeader[i][2] || '',
            title: parseInt(recordsWithoutHeader[i][3]) || 0,
            name: parseInt(recordsWithoutHeader[i][4]) || 0,
            surname: recordsWithoutHeader[i][5] || '',
            title_en: recordsWithoutHeader[i][6] || '',
            name_en: recordsWithoutHeader[i][7] || '',
            surname_en: recordsWithoutHeader[i][8] || '',
            national_id: parseInt(recordsWithoutHeader[i][9]) || 0,
            national_id_expiry_date: recordsWithoutHeader[i][10] || '',
            birthdate: parseInt(recordsWithoutHeader[i][11]) || 0,
            email: parseInt(recordsWithoutHeader[i][12]) || 0,
            tel: parseInt(recordsWithoutHeader[i][13]) || 0,
            marital_status: parseInt(recordsWithoutHeader[i][14]) || 0,
            spouse_title: parseInt(recordsWithoutHeader[i][15]) || 0,
            spouse_name: parseInt(recordsWithoutHeader[i][16]) || 0,
            spouse_surname: parseInt(recordsWithoutHeader[i][17]) || 0,
            spouse_title_en: parseInt(recordsWithoutHeader[i][18]) || 0,
            spouse_name_en: parseInt(recordsWithoutHeader[i][19]) || 0,
            spouse_surname_en: parseInt(recordsWithoutHeader[i][20]) || 0,
            gender: parseInt(recordsWithoutHeader[i][21]) || 0,
            education: parseInt(recordsWithoutHeader[i][22]) || 0,
            sources_of_income_list: parseInt(recordsWithoutHeader[i][23]) || 0,
            occupation: parseInt(recordsWithoutHeader[i][24]) || 0,
            business_type: parseInt(recordsWithoutHeader[i][25]) || 0,
            business_type_other: parseInt(recordsWithoutHeader[i][26]) || 0,
            work_place: parseInt(recordsWithoutHeader[i][27]) || 0,
            job_title: parseInt(recordsWithoutHeader[i][28]) || 0,
            income: parseInt(recordsWithoutHeader[i][29]) || 0,
            income_sources_country: parseInt(recordsWithoutHeader[i][30]) || 0,
            investment_objective: parseInt(recordsWithoutHeader[i][31]) || 0,
            id_card_address_house_no: parseInt(recordsWithoutHeader[i][32]) || 0,
            id_card_address_village_no: parseInt(recordsWithoutHeader[i][33]) || 0,
            id_card_address_building: parseInt(recordsWithoutHeader[i][34]) || 0,
            id_card_address_room_no: parseInt(recordsWithoutHeader[i][35]) || 0,
            id_card_address_floor: parseInt(recordsWithoutHeader[i][36]) || 0,
            id_card_address_lane: parseInt(recordsWithoutHeader[i][37]) || 0,
            id_card_address_road: parseInt(recordsWithoutHeader[i][38]) || 0,
            id_card_address_sub_district: parseInt(recordsWithoutHeader[i][39]) || 0,
            id_card_address_district: parseInt(recordsWithoutHeader[i][40]) || 0,
            id_card_address_province: parseInt(recordsWithoutHeader[i][41]) || 0,
            id_card_address_postal_code: parseInt(recordsWithoutHeader[i][42]) || 0,
            id_card_address_country: parseInt(recordsWithoutHeader[i][43]) || 0,
            
            current_address_house_no: parseInt(recordsWithoutHeader[i][44]) || 0,
            current_address_village_no: parseInt(recordsWithoutHeader[i][45]) || 0,
            current_address_building: parseInt(recordsWithoutHeader[i][46]) || 0,
            current_address_room_no: parseInt(recordsWithoutHeader[i][47]) || 0,
            current_address_floor: parseInt(recordsWithoutHeader[i][48]) || 0,
            current_address_lane: parseInt(recordsWithoutHeader[i][49]) || 0,
            current_address_road: parseInt(recordsWithoutHeader[i][50]) || 0,
            current_address_sub_district: parseInt(recordsWithoutHeader[i][51]) || 0,
            current_address_district: parseInt(recordsWithoutHeader[i][52]) || 0,
            current_address_province: parseInt(recordsWithoutHeader[i][53]) || 0,
            current_address_postal_code: parseInt(recordsWithoutHeader[i][54]) || 0,
            current_address_country: parseInt(recordsWithoutHeader[i][55]) || 0,

            workplace_address_house_no: parseInt(recordsWithoutHeader[i][56]) || 0,
            workplace_address_village_no: parseInt(recordsWithoutHeader[i][57]) || 0,
            workplace_address_building: parseInt(recordsWithoutHeader[i][58]) || 0,
            workplace_address_room_no: parseInt(recordsWithoutHeader[i][59]) || 0,
            workplace_address_floor: parseInt(recordsWithoutHeader[i][60]) || 0,
            workplace_address_lane: parseInt(recordsWithoutHeader[i][61]) || 0,
            workplace_address_road: parseInt(recordsWithoutHeader[i][62]) || 0,
            workplace_address_sub_district: parseInt(recordsWithoutHeader[i][63]) || 0,
            workplace_address_district: parseInt(recordsWithoutHeader[i][64]) || 0,
            workplace_address_province: parseInt(recordsWithoutHeader[i][65]) || 0,
            workplace_address_postal_code: parseInt(recordsWithoutHeader[i][66]) || 0,
            workplace_address_country: parseInt(recordsWithoutHeader[i][67]) || 0,

            contact_address_house_no: parseInt(recordsWithoutHeader[i][68]) || 0,
            contact_address_village_no: parseInt(recordsWithoutHeader[i][69]) || 0,
            contact_address_building: parseInt(recordsWithoutHeader[i][70]) || 0,
            contact_address_room_no: parseInt(recordsWithoutHeader[i][71]) || 0,
            contact_address_floor: parseInt(recordsWithoutHeader[i][72]) || 0,
            contact_address_lane: parseInt(recordsWithoutHeader[i][73]) || 0,
            contact_address_road: parseInt(recordsWithoutHeader[i][74]) || 0,
            contact_address_sub_district: parseInt(recordsWithoutHeader[i][75]) || 0,
            contact_address_district: parseInt(recordsWithoutHeader[i][76]) || 0,
            contact_address_province: parseInt(recordsWithoutHeader[i][77]) || 0,
            contact_address_postal_code: parseInt(recordsWithoutHeader[i][78]) || 0,
            contact_address_country: parseInt(recordsWithoutHeader[i][79]) || 0,

            oa_status: parseInt(recordsWithoutHeader[i][80]) || 0,
            open_date: parseInt(recordsWithoutHeader[i][81]) || 0,
            oa_reject_reason: parseInt(recordsWithoutHeader[i][82]) || 0,
            updated_at: new Date()
          }

          collection.updateOne(
            {
              user_id: doc['user_id'],
            },
            {
              $set: {
                updated_at: doc['updated_at']
              },
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
