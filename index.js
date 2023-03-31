import minimist from 'minimist'
import * as migration from './migration/index.js'

const argv = minimist(process.argv.slice(2))
delete argv['_']
const missing = []

if (
	!argv.migrate ||
	!argv.db ||
	!argv.collection ||
	!argv.uri ||
	(!argv.path && argv.migrate != 'partners') ||
	typeof argv.migrate != 'string' ||
	typeof argv.db != 'string' ||
	typeof argv.collection != 'string' ||
	typeof argv.uri != 'string' ||
	(typeof argv.path != 'string' && argv.migrate != 'partners')
) {
	if (!argv.migrate || typeof argv.migrate != 'string') {
		missing.push('--migrate')
	}
	if (!argv.db || typeof argv.db != 'string') {
		missing.push('--db')
	}
	if (!argv.collection || typeof argv.collection != 'string') {
		missing.push('--collection')
	}
	if (!argv.uri || typeof argv.uri != 'string') {
		missing.push('--uri')
	}
	if (!argv.path || typeof argv.path != 'string') {
		missing.push('--path')
	}
	console.log(`Missing or Invalid arguments: ${missing}`)
	console.log(
		`node index.js --migrate <MIGRATE> --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`
	)
} else {
	switch (argv.migrate) {
		case 'auth-email':
			migration.email(argv.db, argv.collection, argv.uri, argv.path)
			break
		case 'port':
			migration.port(argv.db, argv.collection, argv.uri, argv.path)
			break
		case 'partners':
			migration.deletePartners(argv.db, argv.collection, argv.uri)
			break
		default:
			console.log('Invalid migrate value:', [
				'auth-email',
				'port',
				'partners'
			])
	}
}
