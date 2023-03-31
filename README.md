# customer-data-migration

### install dependencies

`npm i`

<br/>

# Migration

### migrate auth-email

`npm start --migrate auth-email --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate port data

`npm start --migrate port --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### delete partners

`npm start --migrate partners --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`
