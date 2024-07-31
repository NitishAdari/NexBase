import { Client } from 'pg';


class NEXBASE{
    #databaseClientA; 
    #databaseClientB;
    #databaseClientC;
    #databaseClientD;
    #isConnected = false;
    #intervalId = NULL;
    #taskQueue = [];
    #isProcessing = false;


    constructor(url1, url2, url3, url4) {
        this.databaseClientA = new Client({ connectionString: url1 });
        this.databaseClientB = new Client({ connectionString: url2 });
        this.databaseClientC = new Client({ connectionString: url3 });
        this.databaseClientD = new Client({ connectionString: url4 });

        this.isConnected = false;
        this.taskQueue = [];
        this.isProcessing = false;

        // Initialize database connections and table creation
        // this.connect()
        //     .then(() => console.log('Connected and tables created'))
        //     .catch(err => console.error('Error connecting:', err));
    }

    async connect() {
        try {
            // Connect to all database clients asynchronously
            await this.databaseClientA.connect();
            await this.databaseClientB.connect();
            await this.databaseClientC.connect();
            await this.databaseClientD.connect();

            // Define the query to create a table if it does not exist
            const createTableQuery = {
                text: "CREATE TABLE IF NOT EXISTS key_value_store(identifier VARCHAR(255) PRIMARY KEY, value TEXT, expiration INTEGER);"
            };

            // Execute the create table query on all database clients
            await this.databaseClientA.query(createTableQuery);
            await this.databaseClientB.query(createTableQuery);
            await this.databaseClientC.query(createTableQuery);
            await this.databaseClientD.query(createTableQuery);

            // Set isConnected flag to true indicating successful connection
            this.isConnected = true;

            // Start processing the task queue after successful connection
            this.processTaskQueue();
        } catch (err) {
            // Throw an error if there's any issue during connection or table creation
            throw new Error(`\n\nError during table creation: ${err}\n\n`);
        }
    }

    async processTaskQueue() {
        if (!this.isProcessing && this.taskQueue.length > 0) {
            this.isProcessing = true;
            const task = this.taskQueue.shift();

            try {
                switch (task.type) {
                    case 'get':
                        await this.getValue(task.identifier);
                        break;
                    case 'set':
                        await this.setValue(task.identifier, task.value, task.ttl);
                        break;
                    case 'delete':
                        await this.deleteValue(task.identifier);
                        break;
                    case 'expire':
                        await this.setExpiry(task.identifier, task.ttl);
                        break;
                    case 'getAllKeys':
                        await this.getAllKeys();
                        break;
                    case 'countKeys':
                        await this.countKeys();
                        break;
                    case 'deleteAllExpired':
                        await this.deleteAllExpired();
                        break;
                    default:
                        console.warn('Unknown task type:', task.type);
                }
            } catch (err) {
                console.error('Error processing task:', err);
            }

            this.isProcessing = false;
            setTimeout(() => this.processTaskQueue(), 0); // Process next task asynchronously
        }
    }

    addToTaskQueue(type, identifier, value, ttl) {
        this.taskQueue.push({ type, identifier, value, ttl });
        if (!this.isProcessing) {
            this.processTaskQueue();
        }
    }

    async getValue(identifier) {
        this.checkConnection();
        if (identifier) {
            const selectQuery = {
                text: "SELECT value FROM key_value_store WHERE identifier=$1 AND expiration > EXTRACT(epoch FROM NOW());",
                values: [identifier],
            };

            try {
                let client;
                const initialLetter = identifier.charAt(0).toLowerCase();
                if (initialLetter >= 'a' && initialLetter <= 'g') {
                    client = this.databaseClientA;
                } else if (initialLetter >= 'h' && initialLetter <= 'm') {
                    client = this.databaseClientB;
                } else if (initialLetter >= 'n' && initialLetter <= 't') {
                    client = this.databaseClientC;
                } else {
                    client = this.databaseClientD;
                }

                const result = await client.query(selectQuery);

                if (result.rows.length > 0) {
                    console.log(`Value for ${identifier}:`, result.rows[0].value);
                } else {
                    console.error(`${identifier} not found!!`);
                }
            } catch (err) {
                console.error(`Error executing GET query: ${err.message}`);
            }
        } else {
            console.error("Invalid identifier. Please provide a valid identifier.");
        }
    }

    async setValue(identifier, value, ttl) {
        this.checkConnection();

        const currentEpochTime = Math.floor(new Date().getTime() / 1000);
        ttl = ttl + currentEpochTime;

        if (identifier && value && ttl) {
            const insertQuery = {
                text: "INSERT INTO key_value_store (identifier, value, expiration) VALUES ($1, $2, $3) ON CONFLICT (identifier) DO UPDATE SET identifier = $1, value = $2, expiration = $3;",
                values: [identifier, value, ttl],
            };

            try {
                let client;
                const initialLetter = identifier.charAt(0).toLowerCase();
                if (initialLetter >= 'a' && initialLetter <= 'g') {
                    client = this.databaseClientA;
                } else if (initialLetter >= 'h' && initialLetter <= 'm') {
                    client = this.databaseClientB;
                } else if (initialLetter >= 'n' && initialLetter <= 't') {
                    client = this.databaseClientC;
                } else {
                    client = this.databaseClientD;
                }

                const result = await client.query(insertQuery);

                if (result.rowCount > 0) {
                    console.log(`Value ${value} set for identifier ${identifier}`);
                } else {
                    console.error(`${identifier} not found!!`);
                }
            } catch (err) {
                console.error(`Error executing SET query: ${err.message}`);
            }
        } else {
            console.error("Invalid parameters. Please provide valid parameters.");
        }
    }

    async deleteValue(identifier) {
        this.checkConnection();
        if (identifier) {
            const deleteQuery = {
                text: "DELETE FROM key_value_store WHERE identifier=$1;",
                values: [identifier],
            };

            try {
                let client;
                const initialLetter = identifier.charAt(0).toLowerCase();
                if (initialLetter >= 'a' && initialLetter <= 'g') {
                    client = this.databaseClientA;
                } else if (initialLetter >= 'h' && initialLetter <= 'm') {
                    client = this.databaseClientB;
                } else if (initialLetter >= 'n' && initialLetter <= 't') {
                    client = this.databaseClientC;
                } else {
                    client = this.databaseClientD;
                }

                const result = await client.query(deleteQuery);

                if (result.rowCount > 0) {
                    console.log(`Deleted value for identifier ${identifier}`);
                } else {
                    console.error(`${identifier} not found!!`);
                }
            } catch (err) {
                console.error(`Error executing DELETE query: ${err.message}`);
            }
        } else {
            console.error("Invalid identifier. Please provide a valid identifier.");
        }
    }

    async setExpiry(identifier, ttl) {
        this.checkConnection();
        const currentEpochTime = Math.floor(new Date().getTime() / 1000);
        ttl = ttl + currentEpochTime;

        if (identifier && ttl) {
            const updateExpiryQuery = {
                text: "UPDATE key_value_store SET expiration=$1 WHERE identifier=$2;",
                values: [ttl, identifier],
            };

            try {
                let client;
                const initialLetter = identifier.charAt(0).toLowerCase();
                if (initialLetter >= 'a' && initialLetter <= 'g') {
                    client = this.databaseClientA;
                } else if (initialLetter >= 'h' && initialLetter <= 'm') {
                    client = this.databaseClientB;
                } else if (initialLetter >= 'n' && initialLetter <= 't') {
                    client = this.databaseClientC;
                } else {
                    client = this.databaseClientD;
                }

                const result = await client.query(updateExpiryQuery);

                if (result.rowCount > 0) {
                    console.log(`Set expiry for identifier ${identifier} with TTL ${ttl}`);
                } else {
                    console.error(`${identifier} not found!!`);
                }
            } catch (err) {
                console.error(`Error executing EXPIRE query: ${err.message}`);
            }
        } else {
            console.error("Invalid parameters. Please provide valid parameters.");
        }
    }

    async getAllKeys() {
        this.checkConnection();
        const getAllKeysQuery = {
            text: "SELECT identifier FROM key_value_store;",
        };

        try {
            const resultA = await this.databaseClientA.query(getAllKeysQuery);
            const resultB = await this.databaseClientB.query(getAllKeysQuery);
            const resultC = await this.databaseClientC.query(getAllKeysQuery);
            const resultD = await this.databaseClientD.query(getAllKeysQuery);

            const allKeys = [
                ...resultA.rows.map(row => row.identifier),
                ...resultB.rows.map(row => row.identifier),
                ...resultC.rows.map(row => row.identifier),
                ...resultD.rows.map(row => row.identifier)
            ];

            console.log("All keys:", allKeys);
            return allKeys;
        } catch (err) {
            console.error(`Error executing GET ALL KEYS query: ${err.message}`);
        }
    }

    async countKeys() {
        this.checkConnection();
        const countKeysQuery = {
            text: "SELECT COUNT(identifier) FROM key_value_store;",
        };

        try {
            const resultA = await this.databaseClientA.query(countKeysQuery);
            const resultB = await this.databaseClientB.query(countKeysQuery);
            const resultC = await this.databaseClientC.query(countKeysQuery);
            const resultD = await this.databaseClientD.query(countKeysQuery);

            const totalCount = resultA.rows[0].count
                             + resultB.rows[0].count
                             + resultC.rows[0].count
                             + resultD.rows[0].count;

            console.log("Total number of keys:", totalCount);
            return totalCount;
        } catch (err) {
            console.error(`Error executing COUNT KEYS query: ${err.message}`);
        }
    }

    async deleteAllExpired() {
        this.checkConnection();
        const currentEpochTime = Math.floor(new Date().getTime() / 1000);
        const deleteExpiredQuery = {
            text: "DELETE FROM key_value_store WHERE expiration <= $1;",
            values: [currentEpochTime],
        };

        try {
            const resultA = await this.databaseClientA.query(deleteExpiredQuery);
            const resultB = await this.databaseClientB.query(deleteExpiredQuery);
            const resultC = await this.databaseClientC.query(deleteExpiredQuery);
            const resultD = await this.databaseClientD.query(deleteExpiredQuery);

            const totalDeleted = resultA.rowCount
                               + resultB.rowCount
                               + resultC.rowCount
                               + resultD.rowCount;

            console.log("Total expired keys deleted:", totalDeleted);
            return totalDeleted;
        } catch (err) {
            console.error(`Error executing DELETE EXPIRED KEYS query: ${err.message}`);
        }
    }

    #checkConnection() {
        if (!this.isConnected) {
            throw new Error("Call connect() first.");
        }
    }

    async disconnect(){
        this.checkConnection();
        try {
            if (this.intervalId !== null) {
                clearInterval(this.intervalId);
                this.intervalId = null;
            }
            this.databaseClientA.end();
            this.databaseClientB.end();
            this.databaseClientC.end();
            this.databaseClientD.end();
            this.isConnected = false;
        } catch (err) {
            throw new Error(`\n\nError disconnecting: ${err}\n\n`);
        }
    }
}

export default NEXBASE;
