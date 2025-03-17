# customer-data-migration

### install dependencies

`npm i`

# Migration

### migrate auth-email

`node index.js --migrate auth-email --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate port

`node index.js --migrate port --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate identity

`node index.js --migrate identity --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate accounts

`node index.js --migrate accounts --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate ever_approved

`node index.js --migrate ever-approved --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate banks

`node index.js --migrate banks --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate open_account_at

`node index.js --migrate open-account-at --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate plans

`node index.js --migrate plans --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate advisor

`node index.js --migrate advisor --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI> --path <PATH_TO_CSV>`

### migrate remove-subscriber

`node index.js --migrate remove-subscriber --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate external_user_id

`node index.js --migrate external-user-id --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate registered_at

`node index.js --migrate registered-at --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate bank_is_main

`node index.js --migrate bank-is-main --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate robo advisor

`node index.js --migrate robo-advisor --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`

### migrate external_user_id is duplicated

`node index.js --migrate remove-external-user-id-dup --db <MONGO_DB> --collection <MONGO_COLLECTION> --uri <MONGO_URI>`
