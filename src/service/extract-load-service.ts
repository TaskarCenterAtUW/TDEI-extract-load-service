import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import AdmZip, { IZipEntry } from 'adm-zip';
import { Utility } from "../utility/utility";
import { environment } from "../environment/environment";
import { PermissionRequest } from "nodets-ms-core/lib/core/auth/model/permission_request";
import { PoolClient } from "pg";


export class ExtractLoadRequest {
    tdei_project_group_id!: string;
    data_type!: string;
    file_upload_path!: string;
    user_id!: string;
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
        const user_id = extractLoadRequest.user_id;
        const data_type = extractLoadRequest.data_type;
        const tdei_project_group_id = extractLoadRequest.tdei_project_group_id;

        const allowedRoles: { [key: string]: string[] } = {
            "osw": ["tdei_admin", "poc", "osw_data_generator"],
            "flex": ["tdei_admin", "poc", "flex_data_generator"],
            "pathways": ["tdei_admin", "poc", "pathways_data_generator"]
        }
        //validate the inputs
        if (!["osw", "flex", "pathways"].includes(data_type)) {
            await this.publishMessage(message, false, "Invalid data type");
            return false;
        }

        //authorize the user
        const authProvider = Core.getAuthorizer({ provider: "Hosted", apiUrl: environment.authPermissionUrl });
        const permissionRequest = new PermissionRequest({
            userId: user_id as string,
            projectGroupId: tdei_project_group_id,
            permssions: allowedRoles[data_type],
            shouldSatisfyAll: false
        });

        const response = await authProvider?.hasPermission(permissionRequest);
        if (!response) {
            await this.publishMessage(message, false, "Unauthorized user");
            return false;
        }

        const storageClient = Core.getStorageClient();
        const fileEntity = await storageClient!.getFileFromUrl(fileUploadPath);
        const readStream = await fileEntity.getStream();

        switch (data_type) {
            case "osw":
                await this.processOSWDataset(message, readStream);
                break;
            case "flex":
                await this.processFlexDataset(message, readStream);
                break;
            case "pathways":
                await this.processPathwaysDataset(message, readStream);
                break;
        }

        return true;
    }

    private async processPathwaysDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
        throw new Error("Method not implemented.");
    }
    private async processFlexDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
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
    private async processOSWDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
        const tdei_dataset_id = message.messageId;
        const user_id = message.data.user_id;
        const zip = new AdmZip(await Utility.stream2buffer(readStream));
        try {

            await dbClient.runInTransaction(async (client) => {
                // Execute multiple queries within the transactional scope 
                const promises = zip.getEntries().map((entry: IZipEntry) => {
                    if (!entry.isDirectory) {
                        const content = entry.getData().toString('utf8');
                        if (entry.entryName.endsWith('.geojson')) {
                            const jsonData = JSON.parse(content);
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
                            }
                        }
                    }
                });
                await Promise.all(promises);
            });

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
 * Inserts a batch of edge records into the 'content.edge' table.
 * 
 * @param client - The database client used to execute the query.
 * @param tdei_dataset_id - The ID of the dataset.
 * @param user_id - The ID of the user who requested the insertion.
 * @param jsonData - The JSON data containing the edge records.
 * @returns A Promise that resolves to void.
 * @throws An error if there is an issue inserting the edge records.
 */
    private async bulkInsertEdges(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
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
    private async bulkInsertNodes(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
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
    private async bulkInsertPoints(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
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
    private async bulkInsertPolygons(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
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
    private async bulkInsertLines(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
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
    private async publishMessage(message: QueueMessage, success: boolean, resText: string) {
        var data = {
            message: resText,
            success: success,
            data_type: message.data.data_type,
            file_upload_path: message.data.file_upload_path
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.extractLoadResponseTopic as string).publish(message);
    }

}

const extractLoadService: IExtractLoadRequest = new ExtractLoadService();
export default extractLoadService;


