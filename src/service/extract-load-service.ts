import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import { environment } from "../environment/environment";
import { PoolClient } from "pg";
import unzipper from 'unzipper';
import path from 'path';
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

    public async getFileStream(fileUploadPath: string) {
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
                const directory = (await this.getFileStream(fileUploadPath)).pipe(unzipper.Parse({ forceStream: true }));

                const promises = [];
                console.time(`processFiles ${tdei_dataset_id}`);
                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.geojson') && !entry.path.includes('__MACOSX/')) {
                        const content = await entry.buffer();
                        let jsonData;
                        try {
                            jsonData = JSON.parse(content.toString('utf8'));
                        } catch (error) {
                            console.error("Unable to parse content as JSON:", entry.path, error);
                            throw new Error("Unable to parse content as JSON:" + entry.path + error);
                        }

                        if (entry.path.includes('nodes')) {
                            promises.push(this.bulkInsertNodes(client, tdei_dataset_id, user_id, jsonData));
                        } else if (entry.path.includes('edges')) {
                            promises.push(this.bulkInsertEdges(client, tdei_dataset_id, user_id, jsonData));
                        } else if (entry.path.includes('points')) {
                            promises.push(this.bulkInsertPoints(client, tdei_dataset_id, user_id, jsonData));
                        } else if (entry.path.includes('lines')) {
                            promises.push(this.bulkInsertLines(client, tdei_dataset_id, user_id, jsonData));
                        } else if (entry.path.includes('polygons')) {
                            promises.push(this.bulkInsertPolygons(client, tdei_dataset_id, user_id, jsonData));
                        } else if (entry.path.includes('zones')) {
                            promises.push(this.bulkInsertZones(client, tdei_dataset_id, user_id, jsonData));
                        } else {
                            //Process geojson as an extension file if it does not match any of the above
                            promises.push(this.bulkInsertExtension(client, tdei_dataset_id, user_id, jsonData, entry));
                        }

                    }
                }
                await Promise.all(promises);
                console.timeEnd(`processFiles ${tdei_dataset_id}`);
            });

            console.log(`Data loaded successfully for dataset: ${tdei_dataset_id}`);
            console.log(`Updating osw statistics for dataset: ${tdei_dataset_id}`);
            console.time(`updateOSWStats ${tdei_dataset_id}`);
            //Update osw statistics
            const queryObject = {
                text: `
                    SELECT content.tdei_update_osw_stats($1)
                `,
                values: [tdei_dataset_id]
            };
            await dbClient.query(queryObject);
            console.timeEnd(`updateOSWStats ${tdei_dataset_id}`);
            console.log(`OSW statistics updated successfully for dataset: ${tdei_dataset_id}`);
            // All successful
            await this.publishMessage(message, true, "Data loaded successfully");
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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Strip Z coordinates from geometries
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'zones');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

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
            console.time(`bulkInsertEdges ${tdei_dataset_id}`);
            console.log('Inserting edge records');
            // Batch processing
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Strip Z coordinates from geometries
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'edges');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.edge (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(`bulkInsertEdges ${tdei_dataset_id}`);
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting edge records:', error);
            throw new Error('Error inserting edge records:' + error);
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
    public async bulkInsertExtension(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any, entry: any): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            //store additional information
            const ext_file_id = await this.updateExtensionFileData(jsonData, tdei_dataset_id, user_id, entry.path, client);
            console.time(`bulkInsertExtension ${tdei_dataset_id}`);
            console.log('Inserting extension records');
            // Batch processing
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Parameterized query
                const values = batch.flatMap((record: any) => [tdei_dataset_id, ext_file_id, record, user_id]);
                const placeholders = batch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.extension (tdei_dataset_id, ext_file_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(`bulkInsertExtension ${tdei_dataset_id}`);
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting extension records:', error);
            throw new Error('Error inserting extension records:' + error);
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

    public async updateExtensionFileData(jsonData: any, tdei_dataset_id: string, user_id: string, file_name: string, client: PoolClient): Promise<number> {

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
                INSERT INTO content.extension_file (tdei_dataset_id, name, file_meta, requested_by) 
                VALUES ($1, $2, $3, $4) RETURNING id
            `,
            values: [tdei_dataset_id, path.parse(file_name).name, additionalInfo, user_id]
        };

        let result = await dbClient.executeQuery(client, queryObject);
        return result.rows[0].id;
    }

    /**
     * Counts existing ext:elevation* properties in feature properties
     * @param properties - Feature properties object
     * @returns Count of existing ext:elevation* properties
     */
    private countExistingElevationProperties(properties: any): number {
        if (!properties || typeof properties !== 'object') {
            return 0;
        }

        let count = 0;
        for (const key in properties) {
            if (key.startsWith('ext:elevation')) {
                count++;
            }
        }
        return count;
    }

    /**
     * Strips Z coordinate from geometry coordinates to make it 2D
     * @param coordinates - Geometry coordinates (can be nested arrays)
     * @returns 2D coordinates without Z dimension
     */
    private stripZCoordinate(coordinates: any): any {
        if (!Array.isArray(coordinates)) {
            return coordinates;
        }

        // Check if this is a coordinate pair/triple [x, y] or [x, y, z]
        if (coordinates.length >= 2 && typeof coordinates[0] === 'number' && typeof coordinates[1] === 'number') {
            // This is a coordinate pair/triple - return only [x, y]
            return [coordinates[0], coordinates[1]];
        }

        // This is an array of coordinates - recursively process each
        return coordinates.map((coord: any) => this.stripZCoordinate(coord));
    }

    /**
     * Extracts a single elevation value (Z coordinate) from geometry coordinates
     * Used for nodes/points to store elevation as a property
     * Returns the first Z coordinate found in the geometry
     * @param coords - Geometry coordinates (can be nested arrays)
     * @returns First Z coordinate value found, or null if none found
     */
    private extractElevationValue(coords: any): number | null {
        if (!Array.isArray(coords)) {
            return null;
        }

        // Check if this is a coordinate pair/triple [x, y] or [x, y, z]
        if (coords.length >= 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            // This is a coordinate pair/triple
            if (coords.length >= 3 && typeof coords[2] === 'number') {
                return coords[2]; // Found Z coordinate
            }
            return null; // No Z coordinate in this pair
        }

        // This is an array of coordinates - recursively search each
        for (const coord of coords) {
            const z = this.extractElevationValue(coord);
            if (z !== null) {
                return z;
            }
        }

        return null;
    }

    /**
     * Extracts elevation (Z coordinate) from GeoJSON geometry for nodes/points
     * For other types, strips Z coordinate to make 2D geometry
     * @param feature - GeoJSON feature object
     * @param featureType - Type of feature: 'nodes' | 'points' | 'edges' | 'lines' | 'polygons' | 'zones'
     * @returns Modified feature with elevation property (for nodes/points) or 2D geometry (for others)
     */
    private processGeometryElevation(feature: any, featureType: string): any {
        if (!feature || !feature.geometry || !feature.geometry.coordinates) {
            return feature;
        }

        const geometry = feature.geometry;
        const coordinates = geometry.coordinates;
        const isNodeOrPoint = featureType === 'nodes' || featureType === 'points';

        try {
            if (isNodeOrPoint) {
                // For nodes and points: extract elevation from any geometry type
                // Find first Z coordinate in the geometry
                const elevation = this.extractElevationValue(coordinates);

                // Strip Z coordinates from geometry - DB stores 2D only
                feature.geometry.coordinates = this.stripZCoordinate(coordinates);

                // Add elevation property if found
                if (elevation !== null) {
                    if (!feature.properties) {
                        feature.properties = {};
                    }

                    // Count existing ext:elevation* properties
                    const existingCount = this.countExistingElevationProperties(feature.properties);

                    // Determine property name: ext:elevation, ext:elevation_1, ext:elevation_2, etc.
                    const propertyName = existingCount === 0
                        ? 'ext:elevation'
                        : `ext:elevation_${existingCount}`;

                    feature.properties[propertyName] = elevation;
                }
            } else {
                // For edges, lines, polygons, zones: strip Z coordinate to make 2D
                feature.geometry.coordinates = this.stripZCoordinate(coordinates);
            }
        } catch (error) {
            // If processing fails, return feature as-is
            console.error('Error processing geometry elevation:', error);
        }

        return feature;
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
            console.time(`bulkInsertNodes ${tdei_dataset_id}`);
            console.log('Inserting node records');

            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Extract elevation from geometries and add ext:elevation property (nodes only)
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'nodes');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.node (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(`bulkInsertNodes ${tdei_dataset_id}`);
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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Extract elevation from geometries and add ext:elevation property (points only)
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'points');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Strip Z coordinates from geometries (polygons store 2D only)
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'polygons');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Strip Z coordinates from geometries
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'lines');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, record, user_id]);
                const placeholders = processedBatch.map((_: any, index: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

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


