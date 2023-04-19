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
        const records = data.replace(',000', '+').split('\n').map((line) => line.split(','))
        const recordsWithoutHeader = records.slice(1)
        let updatedDocuments = 0

        for (let i = 0; i < recordsWithoutHeader.length; i++) {
          let doc = {
            user_id: parseInt(recordsWithoutHeader[i][0]) || 0,
            customer_code: recordsWithoutHeader[i][1] || '',
            customer_type: recordsWithoutHeader[i][2] || '',
            title: recordsWithoutHeader[i][3] || '',
            name: recordsWithoutHeader[i][4]|| '',
            surname: recordsWithoutHeader[i][5] || '',
            title_en: recordsWithoutHeader[i][6] || '',
            name_en: recordsWithoutHeader[i][7] || '',
            surname_en: recordsWithoutHeader[i][8] || '',
            national_id: recordsWithoutHeader[i][9] || '',
            national_id_expiry_date: (recordsWithoutHeader[i][10] == "N/A") ? null : recordsWithoutHeader[i][10] || '',
            birthdate: recordsWithoutHeader[i][11] || '',
            email: recordsWithoutHeader[i][12] || '',
            tel: recordsWithoutHeader[i][13] || '',
            marital_status: recordsWithoutHeader[i][14] || '',
            spouse_title: recordsWithoutHeader[i][15] || '',
            spouse_name: recordsWithoutHeader[i][16] || '',
            spouse_surname: recordsWithoutHeader[i][17] || '',
            spouse_title_en: recordsWithoutHeader[i][18] || '',
            spouse_name_en: recordsWithoutHeader[i][19] || '',
            spouse_surname_en: recordsWithoutHeader[i][20] || '',
            gender: recordsWithoutHeader[i][21] || '',
            education: recordsWithoutHeader[i][22] || '',
            sources_of_income_list: recordsWithoutHeader[i][23] || '',
            occupation: recordsWithoutHeader[i][24] || '',
            business_type: recordsWithoutHeader[i][25] || '',
            business_type_other: recordsWithoutHeader[i][26] || '',
            work_place: recordsWithoutHeader[i][27] || '',
            job_title: recordsWithoutHeader[i][28] || '',
            income: recordsWithoutHeader[i][29].replace('+',',000') || '',
            income_sources_country: recordsWithoutHeader[i][30] || '',
            investment_objective: recordsWithoutHeader[i][31] || '',
            id_card_address_house_no: recordsWithoutHeader[i][32] || '',
            id_card_address_village_no: recordsWithoutHeader[i][33] || '',
            id_card_address_building: recordsWithoutHeader[i][34] || '',
            id_card_address_room_no: recordsWithoutHeader[i][35] || '',
            id_card_address_floor: recordsWithoutHeader[i][36] || '',
            id_card_address_lane: recordsWithoutHeader[i][37] || '',
            id_card_address_road: recordsWithoutHeader[i][38] || '',
            id_card_address_sub_district: recordsWithoutHeader[i][39] || '',
            id_card_address_district: recordsWithoutHeader[i][40] || '',
            id_card_address_province: recordsWithoutHeader[i][41] || '',
            id_card_address_postal_code: recordsWithoutHeader[i][42] || '',
            id_card_address_country: recordsWithoutHeader[i][43] || '',
            
            current_address_house_no: recordsWithoutHeader[i][44] || '',
            current_address_village_no: recordsWithoutHeader[i][45] || '',
            current_address_building: recordsWithoutHeader[i][46] || '',
            current_address_room_no: recordsWithoutHeader[i][47] || '',
            current_address_floor: recordsWithoutHeader[i][48] || '',
            current_address_lane: recordsWithoutHeader[i][49] || '',
            current_address_road: recordsWithoutHeader[i][50] || '',
            current_address_sub_district: recordsWithoutHeader[i][51] || '',
            current_address_district: recordsWithoutHeader[i][52] || '',
            current_address_province: recordsWithoutHeader[i][53] || '',
            current_address_postal_code: recordsWithoutHeader[i][54] || '',
            current_address_country: recordsWithoutHeader[i][55] || '',

            workplace_address_house_no: recordsWithoutHeader[i][56] || '',
            workplace_address_village_no: recordsWithoutHeader[i][57] || '',
            workplace_address_building: recordsWithoutHeader[i][58] || '',
            workplace_address_room_no: recordsWithoutHeader[i][59] || '',
            workplace_address_floor: recordsWithoutHeader[i][60] || '',
            workplace_address_lane: recordsWithoutHeader[i][61] || '',
            workplace_address_road: recordsWithoutHeader[i][62] || '',
            workplace_address_sub_district: recordsWithoutHeader[i][63] || '',
            workplace_address_district: recordsWithoutHeader[i][64] || '',
            workplace_address_province: recordsWithoutHeader[i][65] || '',
            workplace_address_postal_code: recordsWithoutHeader[i][66] || '',
            workplace_address_country: recordsWithoutHeader[i][67] || '',

            contact_address_house_no: recordsWithoutHeader[i][68] || '',
            contact_address_village_no: recordsWithoutHeader[i][69] || '',
            contact_address_building: recordsWithoutHeader[i][70] || '',
            contact_address_room_no: recordsWithoutHeader[i][71] || '',
            contact_address_floor: recordsWithoutHeader[i][72] || '',
            contact_address_lane: recordsWithoutHeader[i][73] || '',
            contact_address_road: recordsWithoutHeader[i][74] || '',
            contact_address_sub_district: recordsWithoutHeader[i][75] || '',
            contact_address_district: recordsWithoutHeader[i][76] || '',
            contact_address_province: recordsWithoutHeader[i][77] || '',
            contact_address_postal_code: recordsWithoutHeader[i][78] || '',
            contact_address_country: recordsWithoutHeader[i][79] || '',

            oa_status: recordsWithoutHeader[i][80] || '',
            open_date: recordsWithoutHeader[i][81] || '',
            oa_reject_reason: recordsWithoutHeader[i][82] || '',
            updated_at: new Date()
          }
          let sourceOfIncomeList = doc["sources_of_income_list"].split(",");
          let investmentObj = doc["investment_objective"].split(",");

          collection.updateOne(
            {
              user_id: doc['user_id'],
            },
            {
              $set: {
                befund_customer_id: doc['customer_code'],
                customer_type: doc['customer_type'],
                title: doc['title'],
                name: doc['name'],
                surname: doc['surname'],
                title_en: doc['title_en'],
                name_en: doc['name_en'],
                surname_en: doc['surname_en'],
                "personal_info.national_id": doc['national_id'],
                "personal_info.national_id_expiry_date": doc['national_id_expiry_date'],
                "personal_info.birthdate": doc['birthdate'],
                "personal_info.email": doc['email'],
                "personal_info.tel": doc['tel'],
                "personal_info.marital_status": doc['marital_status'],
                "personal_info.spouse_title": doc['spouse_title'],
                "personal_info.spouse_name": doc['spouse_name'],
                "personal_info.spouse_surname": doc['spouse_surname'],
                "personal_info.spouse_title_en": doc['spouse_title_en'],
                "personal_info.spouse_name_en": doc['spouse_name_en'],
                "personal_info.spouse_surname_en": doc['spouse_surname_en'],
                "personal_info.gender": doc['gender'],
                "personal_info.education": doc['education'],
                "personal_info.occupation": doc['occupation'],
                "personal_info.business_type": doc['business_type'],
                "personal_info.business_type_other": doc['business_type_other'],
                "personal_info.workplace": doc['work_place'],
                "personal_info.job_title": doc['job_title'],
                "personal_info.income": doc['income'],
                "personal_info.income_sources": sourceOfIncomeList,
                "personal_info.income_sources_country": doc['income_sources_country'],
                "personal_info.investment_objective": investmentObj,
                "address.id_card_address.house_no": doc['id_card_address_house_no'],
                "address.id_card_address.village_no": doc['id_card_address_village_no'],
                "address.id_card_address.building": doc['id_card_address_building'],
                "address.id_card_address.room_no": doc['id_card_address_room_no'],
                "address.id_card_address.floor": doc['id_card_address_floor'],
                "address.id_card_address.lane": doc['id_card_address_lane'],
                "address.id_card_address.road": doc['id_card_address_road'],
                "address.id_card_address.sub_district": doc['id_card_address_sub_district'],
                "address.id_card_address.district": doc['id_card_address_district'],
                "address.id_card_address.province": doc['id_card_address_province'],
                "address.id_card_address.postal_code": doc['id_card_address_postal_code'],
                "address.id_card_address.country": doc['id_card_address_country'],
                
                "address.current_address.house_no": doc['current_address_house_no'],
                "address.current_address.village_no": doc['current_address_village_no'],
                "address.current_address.building": doc['current_address_building'],
                "address.current_address.room_no": doc['current_address_room_no'],
                "address.current_address.floor": doc['current_address_floor'],
                "address.current_address.lane": doc['current_address_lane'],
                "address.current_address.road": doc['current_address_road'],
                "address.current_address.sub_district": doc['current_address_sub_district'],
                "address.current_address.district": doc['current_address_district'],
                "address.current_address.province": doc['current_address_province'],
                "address.current_address.postal_code": doc['current_address_postal_code'],
                "address.current_address.country": doc['current_address_country'],

                "address.workplace_address.house_no": doc['workplace_address_house_no'],
                "address.workplace_address.village_no": doc['workplace_address_village_no'],
                "address.workplace_address.building": doc['workplace_address_building'],
                "address.workplace_address.room_no": doc['workplace_address_room_no'],
                "address.workplace_address.floor": doc['workplace_address_floor'],
                "address.workplace_address.lane": doc['workplace_address_lane'],
                "address.workplace_address.road": doc['workplace_address_road'],
                "address.workplace_address.sub_district": doc['workplace_address_sub_district'],
                "address.workplace_address.district": doc['workplace_address_district'],
                "address.workplace_address.province": doc['workplace_address_province'],
                "address.workplace_address.postal_code": doc['workplace_address_postal_code'],
                "address.workplace_address.country": doc['workplace_address_country'],

                "address.contact_address.house_no": doc['contact_address_house_no'],
                "address.contact_address.village_no": doc['contact_address_village_no'],
                "address.contact_address.building": doc['contact_address_building'],
                "address.contact_address.room_no": doc['contact_address_room_no'],
                "address.contact_address.floor": doc['contact_address_floor'],
                "address.contact_address.lane": doc['contact_address_lane'],
                "address.contact_address.road": doc['contact_address_road'],
                "address.contact_address.sub_district": doc['contact_address_sub_district'],
                "address.contact_address.district": doc['contact_address_district'],
                "address.contact_address.province": doc['contact_address_province'],
                "address.contact_address.postal_code": doc['contact_address_postal_code'],
                "address.contact_address.country": doc['contact_address_country'],

                "account.oa_status": doc['oa_status'],
                "account.open_date": doc['open_date'],
                "account.oa_reject_reason": doc['oa_reject_reason'],
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
