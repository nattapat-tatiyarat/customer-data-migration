# customer-data-migration

### install dependencies

`npm i`

# Migration

### migrate auth-email

`node index.js --migrate auth-email --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate port data

`node index.js --migrate port --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### delete partners

`node index.js --migrate partners --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate identity

`node index.js --migrate identity --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`
