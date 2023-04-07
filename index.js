import minimist from 'minimist'
import * as migration from './migration/index.js'

const argv = minimist(process.argv.slice(2))
const { migrate, db, collection, uri, path } = argv
const migrations = {
  'auth-email': migration.email,
  port: migration.port,
  partners: migration.deletePartners,
  identity: migration.identity
}
const migrateFn = migrations[migrate]

if (!migrateFn) {
  console.error(`
    Available value of 'migrate': ${Object.keys(migrations)}
  `)
} else if (
  !migrate ||
  !db ||
  !collection ||
  !uri ||
  (migrate === 'partners' ? false : !path)
) {
  console.error(`
    Missing or Invalid argument(s):
    node index.js --migrate <MIGRATE> --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> ${
      migrate !== 'partners' ? '--path <PATH_TO_CSV>' : ''
    }
  `)
} else {
  migrateFn(db, collection, uri, path)
}
