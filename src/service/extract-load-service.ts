import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import { environment } from "../environment/environment";
import { PoolClient } from "pg";
import unzipper from 'unzipper';
import path from 'path';
import { Readable, Transform, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { chain } from 'stream-chain';
import { parser } from 'stream-json';

import { pick } from 'stream-json/filters/pick.js';
import { streamArray } from 'stream-json/streamers/stream-array.js';
import batch from 'stream-json/utils/batch.js';


type UnzipGeoJsonEntry = Readable & { path: string; type: string };

const CHUNK_SIZE = 64 * 1024; // 64KB chunks

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

    private geoJsonPipeError(err: unknown): Error {
        if (err instanceof Error) {
            if (err.name === 'SyntaxError' || /invalid json/i.test(err.message)) {
                return new Error('The GeoJSON file is not valid JSON: ' + err.message);
            }
            return err;
        }
        return new Error(String(err));
    }

    private async processGeoJsonZipEntry(
        client: PoolClient,
        entry: UnzipGeoJsonEntry,
        tdei_dataset_id: string,
        user_id: string
    ): Promise<void> {
        // Stream once: insert features batch-by-batch while collecting root header pairs for the final metadata update.
        const header: Record<string, any> = {};
        let extFileId: number | undefined;

        const utf8Decode = new Transform({
            transform(chunk: Buffer, _enc, cb) {
                // chunk could be huge from low-compression zip — split it
                let offset = 0;
                while (offset < chunk.length) {
                    const slice = chunk.slice(offset, offset + CHUNK_SIZE);
                    this.push(slice.toString('utf8'));
                    offset += CHUNK_SIZE;
                }
                cb();
            }
        });

        const entryPath = entry.path ?? '';
        const routeKind =
            entryPath.includes('nodes') ? 'nodes' :
                entryPath.includes('edges') ? 'edges' :
                    entryPath.includes('points') ? 'points' :
                        entryPath.includes('lines') ? 'lines' :
                            entryPath.includes('polygons') ? 'polygons' :
                                entryPath.includes('zones') ? 'zones' :
                                    'extension';

        const route: {
            meta: () => Promise<void>;
            insert: (features: any[]) => Promise<void>;
        } = (() => {
            switch (routeKind) {
                case 'nodes':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'node_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertNodes(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'edges':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'event_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertEdges(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'points':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'ext_point_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertPoints(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'lines':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'ext_line_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertLines(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'polygons':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'ext_polygon_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertPolygons(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'zones':
                    return {
                        meta: () => this.updateAdditionalFileData(header, 'zone_info', tdei_dataset_id, client),
                        insert: (features) => this.bulkInsertZones(client, tdei_dataset_id, user_id, { ...header, features }),
                    };
                case 'extension':
                default:
                    return {
                        meta: async () => {
                            extFileId = await this.updateExtensionFileData(header, tdei_dataset_id, user_id, entry.path, client);
                        },
                        insert: (features) => this.bulkInsertExtension(client, tdei_dataset_id, user_id, { ...header, features }, entry, extFileId),
                    };
            }
        })();

        let sawAnyBatch = false;
        let batchCount = 0;

        console.log(`[GeoJSON] pipeline start: ${entryPath}`);

        let depth = 0;
        let currentKey: string | undefined;
        let inKey = false;

        const headerCapture = new Transform({
            objectMode: true,
            transform(data: any, _enc, cb) {
                if (data.name === 'startObject' || data.name === 'startArray') {
                    depth++;
                } else if (data.name === 'endObject' || data.name === 'endArray') {
                    depth--;
                }

                if (depth === 1) {
                    if (data.name === 'startKey') {
                        inKey = true;
                        currentKey = '';
                    } else if (data.name === 'endKey') {
                        inKey = false;
                    } else if (data.name === 'stringChunk' && inKey) {
                        // ONLY accumulate when in key context, never value
                        currentKey += data.value;
                    } else if (data.name === 'stringValue') {
                        // short primitive string value — safe, no buffering risk
                        if (currentKey && currentKey !== 'features' && data.value !== 'FeatureCollection') {
                            header[currentKey] = data.value;
                            console.log(`[GeoJSON] header captured: ${entryPath} -> ${currentKey} = ${data.value}`);
                        }
                        currentKey = undefined;
                    } else if (data.name === 'numberValue') {
                        if (currentKey && currentKey !== 'features') {
                            header[currentKey] = data.value;
                            console.log(`[GeoJSON] header captured: ${entryPath} -> ${currentKey} = ${data.value}`);
                        }
                        currentKey = undefined;
                    }
                } else {
                    // outside depth 1 — always reset key tracking
                    inKey = false;
                }

                cb(null, data);
            }
        });

        const unwrapChunk = new Transform({
            objectMode: true,
            transform(chunk, _enc, cb) {
                cb(null, chunk.value);
            }
        });

        const featureWriter = new Writable({
            objectMode: true,
            write(features: any[], _enc, cb) {
                sawAnyBatch = true;
                batchCount++;
                console.log(`[GeoJSON] batch start: ${entryPath} #${batchCount} size=${features.length}`);

                route.insert(features)
                    .then(() => {
                        console.log(`[GeoJSON] batch done: ${entryPath} #${batchCount}`);
                        cb();
                    })
                    .catch(err => {
                        console.error(`[GeoJSON] batch error: ${entryPath} #${batchCount}`, err);
                        cb(err);
                    });
            },
            final(cb) {
                console.log(`[GeoJSON] features writable final: ${entryPath}, batches=${batchCount}`);
                cb();
            }
        });

        await pipeline(
            entry,
            utf8Decode,
            // Stage 1: parse JSON tokens + capture header — all token-level transforms together
            chain([
                parser(),
                headerCapture,
            ]),
            // Stage 2: extract features array items — pick/streamArray must stay in chain()
            chain([
                pick({ filter: 'features' }),
                streamArray(),
                unwrapChunk,
                batch({ batchSize: environment.bulkInsertSize }),
            ]),
            // Stage 3: write batches
            featureWriter
        );

        console.log(`[GeoJSON] pipeline end: ${entryPath}, sawAnyBatch=${sawAnyBatch}, batches=${batchCount}`);
        if (!sawAnyBatch) await route.insert([]);
        if (routeKind !== 'extension') {
            await route.meta();
        }
    }

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

                let hasGeoJson = false;
                console.time(`processFiles ${tdei_dataset_id}`);
                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.geojson') && !entry.path.includes('__MACOSX/')) {
                        hasGeoJson = true;
                        // Must await each entry: unzipper only advances after this entry's stream is fully consumed.
                        // Starting the next file while a previous entry is still piping can deadlock the parser.
                        await this.processGeoJsonZipEntry(client, entry as UnzipGeoJsonEntry, tdei_dataset_id, user_id);
                    }
                    else {
                        // IMPORTANT: Always drain entries we don't consume, otherwise unzipper's
                        // internal stream can stall and `for await` will appear to "hang".
                        if (typeof (entry as any).autodrain === 'function') {
                            (entry as any).autodrain();
                        } else if (typeof (entry as any).resume === 'function') {
                            (entry as any).resume();
                        }
                    }
                }
                if (!hasGeoJson) {
                    throw new Error('No valid .geojson files found in dataset archive.');
                }
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
            await this.publishMessage(message, false, "Error loading the data : " + (error as Error).message);
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
            const timerLabel = `bulkInsertZones ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.zone (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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
            const timerLabel = `bulkInsertEdges ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.edge (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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
    public async bulkInsertExtension(client: PoolClient, tdei_dataset_id: string, user_id: string, jsonData: any, entry: any, existingExtFileId?: number): Promise<void> {
        const batchSize = environment.bulkInsertSize;
        try {
            //store additional information
            const ext_file_id = existingExtFileId !== undefined
                ? existingExtFileId
                : await this.updateExtensionFileData(jsonData, tdei_dataset_id, user_id, entry.path, client);
            const timerLabel = `bulkInsertExtension ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
            console.log('Inserting extension records');
            // Batch processing
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
                let counter = 1;

                // Strip Z coordinates from geometries (extension files can contain mix of all geometry types)
                const processedBatch = batch.map((record: any) => {
                    return this.processGeometryElevation(record, 'extension');
                });

                // Parameterized query
                const values = processedBatch.flatMap((record: any) => [tdei_dataset_id, ext_file_id, record, user_id]);
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.extension (tdei_dataset_id, ext_file_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
            Promise.resolve(true);
        } catch (error) {
            console.error('Error inserting extension records:', error);
            throw new Error('Error inserting extension records:' + error);
        }
    }

    public async updateAdditionalFileData(jsonData: any, col_name: string, tdei_dataset_id: string, client: PoolClient) {
        console.log(`Updating additional file data for dataset ${tdei_dataset_id}, column ${col_name}`, jsonData);
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

        return Object.keys(properties).filter(key => key.startsWith('ext:elevation')).length;
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
     * Strips Z coordinates and extracts elevation in a single pass
     * This avoids double traversal of the coordinate tree
     * @param coordinates - Geometry coordinates (can be nested arrays)
     * @returns Object with stripped coordinates and extracted elevation value
     */
    private stripZAndExtractElevation(coordinates: any): { stripped: any; elevation: number | null } {
        if (!Array.isArray(coordinates)) {
            return { stripped: coordinates, elevation: null };
        }

        // Check if this is a coordinate pair/triple [x, y] or [x, y, z]
        if (coordinates.length >= 2 && typeof coordinates[0] === 'number' && typeof coordinates[1] === 'number') {
            // This is a coordinate pair/triple
            const stripped = [coordinates[0], coordinates[1]];
            const elevation = (coordinates.length >= 3 && typeof coordinates[2] === 'number')
                ? coordinates[2]
                : null;
            return { stripped, elevation };
        }

        // This is an array of coordinates - recursively process each
        let foundElevation: number | null = null;
        const stripped = coordinates.map((coord: any) => {
            const result = this.stripZAndExtractElevation(coord);
            // Capture first elevation found (short-circuit after first find)
            if (foundElevation === null && result.elevation !== null) {
                foundElevation = result.elevation;
            }
            return result.stripped;
        });

        return { stripped, elevation: foundElevation };
    }

    /**
     * Extracts elevation (Z coordinate) from GeoJSON geometry for nodes/points
     * For other types, strips Z coordinate to make 2D geometry
     * @param feature - GeoJSON feature object
     * @param featureType - Type of feature: 'nodes' | 'points' | 'edges' | 'lines' | 'polygons' | 'zones' | 'extension'
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
                // For nodes and points: extract elevation and strip Z in a single pass
                const { stripped, elevation } = this.stripZAndExtractElevation(coordinates);
                feature.geometry.coordinates = stripped;

                // Add elevation property if found & not 0 
                if (elevation !== null && elevation !== undefined && elevation !== 0) {
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
                // For edges, lines, polygons, zones, extension: strip Z coordinate to make 2D
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
            // Batch processing
            const timerLabel = `bulkInsertNodes ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.node (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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
            const timerLabel = `bulkInsertPoints ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.extension_point (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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
            const timerLabel = `bulkInsertPolygons ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                    INSERT INTO content.extension_polygon (tdei_dataset_id, feature, requested_by)
                    VALUES ${placeholders}
                `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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
            const timerLabel = `bulkInsertLines ${tdei_dataset_id} ${Date.now()}-${Math.random().toString(16).slice(2)}`;
            console.time(timerLabel);
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
                const placeholders = processedBatch.map((_: any) => `($${counter++}, $${counter++}, $${counter++})`).join(', ');

                const queryObject = {
                    text: `
                     INSERT INTO content.extension_line (tdei_dataset_id, feature, requested_by)
                     VALUES ${placeholders}
                 `,
                    values: values
                }

                await dbClient.executeQuery(client, queryObject);
            }
            console.timeEnd(timerLabel);
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


