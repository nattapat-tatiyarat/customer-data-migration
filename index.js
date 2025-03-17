import minimist from 'minimist'
import * as migration from './migration/index.js'

const argv = minimist(process.argv.slice(2))
const { migrate, db, collection, uri, path } = argv
const migrations = {
  'auth-email': migration.email,
  port: migration.port,
  identity: migration.identity,
  accounts: migration.accounts,
  'external-user-id': migration.externalID,
  birthdate: migration.birthdate,
  'ever-approved': migration.everApproved,
  'remove-subscriber': migration.remove_subscriber,
  advisor: migration.advisor,
  banks: migration.bankInfo,
  'open-account-at': migration.openAccAt,
  plans: migration.plans,
  'registered-at': migration.registeredAt,
  'bank-is-main': migration.bankIsMain,
  'robo-advisor': migration.roboAdvisor,
}
const migrateFn = migrations[migrate]

if (!migrateFn) {
  console.error(`
	Available value of 'migrate': ${Object.keys(migrations)}
  `)
}

switch (migrate) {
  case 'remove-subscriber':
    if (!migrate || !db || !collection || !uri) {
      console.error(`
        Missing or Invalid argument(s):
        node index.js --migrate <MIGRATE> --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>
      `)
    } else {
      migrateFn(db, collection, uri)
    }
    break
  default:
    if (!migrate || !db || !collection || !uri || !path) {
      console.error(`
        Missing or Invalid argument(s):
        node index.js --migrate <MIGRATE> --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>
      `)
    } else {
      migrateFn(db, collection, uri, path)
    }
}
