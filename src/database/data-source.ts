import { Pool, PoolClient, QueryConfig, QueryResult } from 'pg';
import { PostgresError } from '../constants/pg-error-constants';
import { environment } from '../environment/environment';
import UniqueKeyDbException, { ForeignKeyDbException } from '../exceptions/db/database-exceptions';

export class DataSource {
    private pool: Pool = new Pool;

    constructor() {
        // TODO document why this constructor is empty

    }

    public initializaDatabase() {
        console.info("Initializing database !");
        this.pool = new Pool({
            database: environment.database.database,
            host: environment.database.host,
            user: environment.database.username,
            password: environment.database.password,
            ssl: environment.database.ssl,
            port: environment.database.port,
            max: environment.database.max,
        });

        this.pool.on('error', function (err: Error) {
            console.log(`Idle-Client Error:\n${err.message}\n${err.stack}`)
        }).on('connect', () => {
            console.log("Database initialized successfully !");
        });
    }

    async beginTransaction(): Promise<PoolClient> {
        const client = await this.pool.connect();
        await client.query('BEGIN');
        return client;
    }

    async commitTransaction(client: PoolClient): Promise<void> {
        try {
            await client.query('COMMIT');
        } finally {
            client.release();
        }
    }

    async rollbackTransaction(client: PoolClient): Promise<void> {
        try {
            await client.query('ROLLBACK');
        } finally {
            client.release();
        }
    }

    async runInTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
        const client = await this.beginTransaction();
        try {
            const result = await callback(client);
            await this.commitTransaction(client);
            return result;
        } catch (error) {
            await this.rollbackTransaction(client);
            throw error;
        }
    }

    async executeQuery(client: PoolClient, queryTextOrConfig: string | QueryConfig<any[]>, params: any[] = []): Promise<QueryResult> {
        if (queryTextOrConfig instanceof String) {
            const result = await client.query(queryTextOrConfig, params);
            return result;
        }
        else {
            const result = await client.query(queryTextOrConfig);
            return result;
        }
    }

    /**
     * Async Query
     * @param sqlText 
     * @param params 
     * @returns 
     */
    async query(queryTextOrConfig: string | QueryConfig<any[]>, params: any[] = []): Promise<QueryResult<any>> {
        const client = await this.pool.connect();
        try {
            if (queryTextOrConfig instanceof String) {
                const result = await client.query(queryTextOrConfig, params);
                return result;
            }
            else {
                const result = await client.query(queryTextOrConfig);
                return result;
            }

        } catch (e: any) {

            switch (e.code) {
                case PostgresError.UNIQUE_VIOLATION:
                    throw new UniqueKeyDbException("Duplicate");
                case PostgresError.FOREIGN_KEY_VIOLATION:
                    throw new ForeignKeyDbException(e.constraint);
                default:
                    break;
            }

            throw e;
        } finally {
            client.release();
        }
    }
}

const dbClient = new DataSource();
export default dbClient;