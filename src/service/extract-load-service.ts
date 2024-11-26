import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import AdmZip, { IZipEntry } from 'adm-zip';
import { Utility } from "../utility/utility";
import { environment } from "../environment/environment";
import { PoolClient } from "pg";


export class ExtractLoadRequest {
    data_type!: string;
    tdei_dataset_id!: string;
    file_upload_path!: string;
    constructor(init: Partial<ExtractLoadRequest>) {
        Object.assign(this, init);
    }
}

export interface IExtractLoadRequest {
    extractLoadRequestProcessor(message: QueueMessage): Promise<boolean>;
}

/**
 * Represents a service for processing extract and load requests.
 */
export class ExtractLoadService {
    constructor() { }

    /**
 * Processes the extract and load request.
 * 
 * @param message - The queue message containing the extract load request.
 * @returns A promise that resolves to a boolean indicating the success of the process.
 */
    public async extractLoadRequestProcessor(message: QueueMessage): Promise<boolean> {
        const extractLoadRequest = message.data as ExtractLoadRequest;
        const fileUploadPath = extractLoadRequest.file_upload_path;
        const data_type = extractLoadRequest.data_type;

        switch (data_type) {
            case "osw":
                await this.processOSWDataset(message, fileUploadPath);
                break;
            case "flex":
                await this.processFlexDataset(message, fileUploadPath);
                break;
            case "pathways":
                await this.processPathwaysDataset(message, fileUploadPath);
                break;
        }

        return true;
    }

    private async getFileStream(fileUploadPath: string) {
        const storageClient = Core.getStorageClient();
        const fileEntity = await storageClient!.getFileFromUrl(fileUploadPath);
        const readStream = await fileEntity.getStream();
        return readStream;
    }

    public async processPathwaysDataset(message: QueueMessage, fileUploadPath: string) {
        throw new Error("Method not implemented.");
    }
    public async processFlexDataset(message: QueueMessage, fileUploadPath: string) {
        throw new Error("Method not implemented.");
    }

    /**
     * Process the OSW dataset.
     * 
     * @param message - The queue message containing the dataset information.
     * @param readStream - The readable stream of the dataset file.
     * @returns A promise that resolves to a boolean indicating the success of the process.
     * @throws An error if there is any issue with loading the data.
     */
    public async processOSWDataset(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('Processing OSW dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records' + tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            await dbClient.runInTransaction(async (client) => {
                const readStream = await this.getFileStream(fileUploadPath);

                let zip = new AdmZip(await Utility.stream2buffer(readStream));
                // Execute multiple queries within the transactional scope 
                const promises = zip.getEntries().map((entry: IZipEntry) => {
                    if (!entry.isDirectory) {
                        const content = entry.getData().toString('utf8');
                        if (entry.entryName.endsWith('.geojson')) {
                            let jsonData;
                            try {
                                jsonData = JSON.parse(content);
                            } catch (error) {
                                console.error("Unable to parse content as JSON:", content);
                                return;
                            }
                            if (entry.entryName.includes('nodes')) {
                                return this.bulkInsertNodes(client, tdei_dataset_id, user_id, jsonData);
                            } else if (entry.entryName.includes('edges')) {
                                return this.bulkInsertEdges(client, tdei_dataset_id, user_id, jsonData);
                            } else if (entry.entryName.includes('points')) {
                                return this.bulkInsertPoints(client, tdei_dataset_id, user_id, jsonData);
                            } else if (entry.entryName.includes('lines')) {
                                return this.bulkInsertLines(client, tdei_dataset_id, user_id, jsonData);
                            } else if (entry.entryName.includes('polygons')) {
                                return this.bulkInsertPolygons(client, tdei_dataset_id, user_id, jsonData);
                            } else if (entry.entryName.includes('zones')) {
                                return this.bulkInsertZones(client, tdei_dataset_id, user_id, jsonData);
                            }
                        }
                    }
                });
                await Promise.all(promises);
            });

            //Update osw statistics
            const queryObject = {
                text: `
                    SELECT content.tdei_update_osw_stats($1)
                `,
                values: [tdei_dataset_id]
            };
            await dbClient.query(queryObject);

            // All successful
            await this.publishMessage(message, true, "Data loaded successfully");
            console.log('Data loaded successfully');
        } catch (error) {
            console.error('Error loading the data:', error);
            // If any of the promises fail, rollback the transaction
            await this.publishMessage(message, false, "Error loading the data" + error);
        }
    }

    /**
* Inserts a batch of zone records into the 'content.zone' table.
* 
* @param client - The database client used to execute the query.
* @param tdei_dataset_id - The ID of the dataset.
* @param user_id - The ID of the user who requested the insertion.
* @param jsonData - The JSON data containing the zone records.
* @returns A Promise that resolves to void.
* @throws An error if there is an issue inserting the zone records.
*/
    public async bulkInsertZones(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "zone_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);

            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;

                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.zone (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting zone records:', error);
            throw new Error('Error inserting zone records:' + error);
        }
    }

    /**
 * Inserts a batch of edge records into the 'content.edge' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the edge records.
 * @returns A Promise that resolves to void.
 * @throws An error if there is an issue inserting the edge records.
 */
    public async bulkInsertEdges(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "event_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);

            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;

                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.edge (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting edge records:', error);
            throw new Error('Error inserting edge records:' + error);
        }
    }

    public async updateAdditionalFileData(jsonData: any, col_name: string, tdei_dataset_id: string, client: PoolClient) {

        const keysToIgnore = ['features', 'type'];
        const additionalInfo: { [key: string]: any } = {};
        Object.entries(jsonData).forEach(([key, value]) => {
            if (!keysToIgnore.includes(key)) {
                additionalInfo[key] = value ?? '';
            }
        });

        //Insert into the dataset table
        const queryObject = {
            text: `
                UPDATE content.dataset SET ${col_name}=$1  WHERE tdei_dataset_id = $2
            `,
            values: [additionalInfo, tdei_dataset_id]
        };

        await dbClient.executeQuery(client, queryObject);
    }

    /**
 * Inserts a batch of node records into the 'content.node' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the node records.
 * @returns A Promise that resolves to void.
 * @throws An error if there is an issue inserting the node records.
 */
    public async bulkInsertNodes(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "node_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.node (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting node records:', error);
            throw new Error('Error inserting node records:' + error);
        }
    }

    /**
 * Inserts a batch of points into the 'content.extension_point' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the points to be inserted.
 * @returns A Promise that resolves to void.
 * @throws An error if there is an issue inserting the points.
 */
    public async bulkInsertPoints(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "ext_point_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.extension_point (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting extension_point records:', error);
            throw new Error('Error inserting extension_point records:' + error);
        }
    }

    /**
 * Inserts polygon records into the 'content.extension_polygon' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the polygon features to be inserted.
 * @returns A Promise that resolves to void.
 * @throws An error if there is an issue inserting the records.
 */
    public async bulkInsertPolygons(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "ext_polygon_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.extension_polygon (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting extension_polygon records:', error);
            throw new Error('Error inserting extension_polygon records:' + error);
        }
    }

    /**
 * Inserts a batch of extension line records into the 'content.extension_line' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the TDEI dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the extension line records.
 * @returns A Promise that resolves to void.
 * @throws Error if there is an error inserting the extension line records.
 */
    public async bulkInsertLines(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            const col_name = "ext_line_info";
            //store additional information
            await this.updateAdditionalFileData(jsonData, col_name, tdei_dataset_id, client);
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.extension_line (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting extension_line records:', error);
            throw new Error('Error inserting extension_line records:' + error);
        }
    }

    /**
 * Publishes a message to the extractLoadResponseTopic topic.
 * 
 * @param message - The original QueueMessage object.
 * @param success - A boolean indicating the success status of the message.
 * @param resText - The response message to be published.
 * @returns A Promise that resolves when the message is published successfully.
 */
    public async publishMessage(message: QueueMessage, success: boolean, resText: string) {
        var data = {
            message: resText,
            success: success
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.extractLoadResponseTopic as string).publish(message);
    }

}

const extractLoadService: IExtractLoadRequest = new ExtractLoadService();
export default extractLoadService;


