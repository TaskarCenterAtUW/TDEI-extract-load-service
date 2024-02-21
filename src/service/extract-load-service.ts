import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import AdmZip from 'adm-zip';
import { Utility } from "../utility/utility";
import { environment } from "../environment/environment";
import { PermissionRequest } from "nodets-ms-core/lib/core/auth/model/permission_request";

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


export class ExtractLoadService {
    constructor() { }

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

        //All successful
        await this.publishMessage(message, true, "Data loaded successfully");

        return true;
    }
    private async processPathwaysDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
        throw new Error("Method not implemented.");
    }
    private async processFlexDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
        throw new Error("Method not implemented.");
    }

    private async processOSWDataset(message: QueueMessage, readStream: NodeJS.ReadableStream) {
        const tdei_dataset_id = message.messageId;
        const user_id = message.data.user_id;
        const zip = new AdmZip(await Utility.stream2buffer(readStream));
        try {
            await dbClient.query('BEGIN');
            zip.forEach(entry => {
                if (!entry.isDirectory) {
                    const content = entry.getData().toString('utf8');
                    if (entry.entryName.endsWith('.geojson')) {
                        const jsonData = JSON.parse(content);
                        if (entry.entryName.includes('nodes')) {
                            this.bulkInsertNodes(tdei_dataset_id, user_id, jsonData);
                        } else if (entry.entryName.includes('edges')) {
                            this.bulkInsertEdges(tdei_dataset_id, user_id, jsonData);
                        } else if (entry.entryName.includes('points')) {
                            this.bulkInsertPoints(tdei_dataset_id, user_id, jsonData);
                        } else if (entry.entryName.includes('lines')) {
                            this.bulkInsertLines(tdei_dataset_id, user_id, jsonData);
                        } else if (entry.entryName.includes('polygons')) {
                            this.bulkInsertPolygons(tdei_dataset_id, user_id, jsonData);
                        }
                    }
                }
            });
            await dbClient.query('COMMIT');
        } catch (error: any) {
            await dbClient.query('ROLLBACK');
            console.error('Error processing dataset:', error);
            await this.publishMessage(message, false, 'Error processing dataset:' + error);
            return false;
        }
    }

    private async bulkInsertEdges(tdei_dataset_id: string, user_id: string, jsonData: any) {
        const batchSize = environment.bulkInsertSize;
        try {
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.map((record: any) => [tdei_dataset_id, record, user_id]);
                const query = `
                    INSERT INTO edge (tdei_dataset_id, feature, requested_by)
                    VALUES ${values.map((_: any, index: number) => `($${counter++},$${counter++},$${counter++})`)
                        .join(', ')}
                `;

                await dbClient.query(query, values.flat());
            }
        } catch (error) {
            console.error('Error inserting edge records:', error);
        }
    }
    private async bulkInsertNodes(tdei_dataset_id: string, user_id: string, jsonData: any) {
        const batchSize = environment.bulkInsertSize;
        try {
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.map((record: any) => [tdei_dataset_id, record, user_id]);
                const query = `
                    INSERT INTO node (tdei_dataset_id, feature, requested_by)
                    VALUES ${values.map((_: any, index: number) => `($${counter++},$${counter++},$${counter++})`)
                        .join(', ')}
                `;

                await dbClient.query(query, values.flat());
            }
        } catch (error) {
            console.error('Error inserting node records:', error);
        }
    }
    private async bulkInsertPoints(tdei_dataset_id: string, user_id: string, jsonData: any) {
        const batchSize = environment.bulkInsertSize;
        try {
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.map((record: any) => [tdei_dataset_id, record, user_id]);
                const query = `
                    INSERT INTO extension_point (tdei_dataset_id, feature, requested_by)
                    VALUES ${values.map((_: any, index: number) => `($${counter++},$${counter++},$${counter++})`)
                        .join(', ')}
                `;

                await dbClient.query(query, values.flat());
            }
        } catch (error) {
            console.error('Error inserting extension_point records:', error);
        }
    }
    private async bulkInsertPolygons(tdei_dataset_id: string, user_id: string, jsonData: any) {
        const batchSize = environment.bulkInsertSize;
        try {
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.map((record: any) => [tdei_dataset_id, record, user_id]);
                const query = `
                    INSERT INTO extension_polygon (tdei_dataset_id, feature, requested_by)
                    VALUES ${values.map((_: any, index: number) => `($${counter++},$${counter++},$${counter++})`)
                        .join(', ')}
                `;

                await dbClient.query(query, values.flat());
            }
        } catch (error) {
            console.error('Error inserting extension_polygon records:', error);
        }
    }
    private async bulkInsertLines(tdei_dataset_id: string, user_id: string, jsonData: any) {
        const batchSize = environment.bulkInsertSize;
        try {
            // Batch processing
            for (let i = 0; i < jsonData.features.length; i += batchSize) {
                const batch = jsonData.features.slice(i, i + batchSize);
                let counter = 1;
                // Parameterized query
                const values = batch.map((record: any) => [tdei_dataset_id, record, user_id]);
                const query = `
                    INSERT INTO extension_line (tdei_dataset_id, feature, requested_by)
                    VALUES ${values.map((_: any, index: number) => `($${counter++},$${counter++},$${counter++})`)
                        .join(', ')}
                `;

                await dbClient.query(query, values.flat());
            }
        } catch (error) {
            console.error('Error inserting extension_line records:', error);
        }
    }
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


