const path = require('path');
const { mergeNoArray } = require('./deepMerge');
const { name: EngineName, version: EngineVersion } = require('./package.json');
const mysql = require('mysql2');
const { StorageType, StorageEngine, StorageFunction, StorageFunctionType, StorageFunctionGroup, KeyFoundError, KeyNotFoundError } = require('@tycrek/ass-storage-engine');

let TABLE = '';
const TIMEOUTS = {
    IDLE: 30000,
    CONNECT: 5000,
};

const OPTIONS = {
    host: 'localhost',
    port: 12345,
    table: 'ass',
    database: 'dbname',
    username: 'dbuser',
    password: 'dbpass',
};

/**
 * Get resource data from the database
 * @param {string} resourceId The resource id
 * @returns {Promise<*>} The resource data
 * @throws {KeyNotFoundError} If the resource is not found
 */
function get(resourceId) {
    return new Promise((resolve, reject) => {
        const query = resourceId
            ? `SELECT * FROM ${TABLE} WHERE id = ?`
            : `SELECT * FROM ${TABLE}`;
        const params = resourceId ? [resourceId] : [];

        MySQLStorageEngine.pool.execute(query, params, (err, results) => {
            if (err) return reject(err);
            if (resourceId && results.length === 0) return reject(new KeyNotFoundError(resourceId));
            resolve(resourceId ? results[0].data : results.map(({ id, data }) => [id, data]));
        });
    });
}

/**
 * Put resource data into the database
 * @param {string} resourceId The resource id
 * @param {Object} resourceData The resource data
 * @returns {Promise<*>}
 * @throws {KeyFoundError} If the resource already exists
 */
function put(resourceId, resourceData) {
    return new Promise((resolve, reject) => {
        has(resourceId)
            .then((exists) => {
                if (exists) throw new KeyFoundError(resourceId);
                const query = `INSERT INTO ${TABLE} (id, data) VALUES (?, ?)`;
                MySQLStorageEngine.pool.execute(query, [resourceId, JSON.stringify(resourceData)], (err) => {
                    if (err) return reject(err);
                    resolve();
                });
            })
            .catch(reject);
    });
}

/**
 * Delete resource data from the database
 * @param {string} resourceId The resource id
 * @returns {Promise<*>}
 * @throws {KeyNotFoundError} If the resource is not found
 */
function del(resourceId) {
    return new Promise((resolve, reject) => {
        const query = `DELETE FROM ${TABLE} WHERE id = ?`;
        MySQLStorageEngine.pool.execute(query, [resourceId], (err, result) => {
            if (err) return reject(err);
            if (result.affectedRows === 0) return reject(new KeyNotFoundError(resourceId));
            resolve();
        });
    });
}

/**
 * Check if resource data exists in the database
 * @param {string} resourceId The resource id
 * @returns {Promise<boolean>}
 */
function has(resourceId) {
    return new Promise((resolve, reject) => {
        const query = `SELECT id FROM ${TABLE} WHERE id = ?`;
        MySQLStorageEngine.pool.execute(query, [resourceId], (err, results) => {
            if (err) return reject(err);
            resolve(results.length > 0);
        });
    });
}

class MySQLStorageEngine extends StorageEngine {
    /**
     * @type {OPTIONS}
     * @private
     */
    #options = {};

    /**
     * @type {mysql.Pool}
     * @public
     * @static
     */
    static pool = null;

    /**
     * @param {OPTIONS} [options] The options to use for the storage engine (optional)
     */
    constructor(options = OPTIONS) {
        super('MySQL', StorageType.DB, new StorageFunctionGroup(
            new StorageFunction(StorageFunctionType.GET, get),
            new StorageFunction(StorageFunctionType.PUT, put),
            new StorageFunction(StorageFunctionType.DEL, del),
            new StorageFunction(StorageFunctionType.HAS, has)
        ));

        this.#options = mergeNoArray(OPTIONS, options);
    }

    /**
     * Initialize the database connection pool
     * @param {StorageEngine} oldEngine The previous storage engine
     * @returns {Promise<*>}
     */
	init(oldEngine) {
		return new Promise((resolve, reject) => {
			const { host, port, database, username: user, password, table } = this.#options;
	
			// Create the pool
			MySQLStorageEngine.pool = mysql.createPool({
				host,
				port,
				user,
				password,
				database,
				waitForConnections: true,
				connectionLimit: 10,
				queueLimit: 0,
			});
	
			// Check if the table exists, and create if not
			const checkTableQuery = `SHOW TABLES LIKE '${table}'`; // No placeholder used here
			MySQLStorageEngine.pool.execute(checkTableQuery, (err, results) => {
				if (err) return reject(err);
	
				if (results.length === 0) {
					const createTableQuery = `
						CREATE TABLE ${table} (
							id VARCHAR(255) PRIMARY KEY,
							data JSON NOT NULL
						)
					`;
					MySQLStorageEngine.pool.execute(createTableQuery, (err) => {
						if (err) return reject(err);
						oldEngine.get()
							.then((oldData) => this.migrate(oldData))
							.then(() => resolve(`Table ${table} created`))
							.catch(reject);
					});
				} else {
					resolve(`Table ${table} exists`);
				}
			});
		});
	}
	

    /**
     * Get the number of keys in the database
     * @returns {Promise<number>}
     */
    get size() {
        return this.#getSize();
    }

    async #getSize() {
        try {
            const [results] = await MySQLStorageEngine.pool.promise().query(`SELECT COUNT(*) AS count FROM ${TABLE}`);
            return results[0].count;
        } catch (err) {
            console.error(err);
            return 0;
        }
    }

    /**
     * Migrate function takes the existing and adds all entries to the database.
     * @param {*[]} data The existing data
     * @returns {Promise<*>}
     */
    migrate(data) {
        return Promise.all(data.map(([key, value]) =>
            has(key).then((exists) => !exists && put(key, value))
        ));
    }

    /**
     * Delete the table
     * @returns {Promise<*>}
     */
    deleteTable() {
        return new Promise((resolve, reject) => {
            const query = `DROP TABLE ${TABLE}`;
            MySQLStorageEngine.pool.execute(query, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }
}

const { host, port, username, password, database, table } = require(path.join(process.cwd(), 'auth.mysql.json'));
TABLE = table;
const assEngine = new MySQLStorageEngine({
    host,
    port,
    username,
    password,
    database,
    table,
});

module.exports = {
    EngineName,
    EngineVersion,
    MySQLStorageEngine,

    _ENGINE_: (oldEngine) => {
        assEngine.init(oldEngine)
            .then(console.log)
            .catch(console.error);
        return assEngine;
    },
};
