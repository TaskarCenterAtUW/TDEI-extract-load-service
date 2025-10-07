import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../database/data-source";
import { Core } from "nodets-ms-core";
import { environment } from "../environment/environment";
import { PoolClient } from "pg";
import unzipper from 'unzipper';
import path from 'path';

import { from as copyFrom } from 'pg-copy-streams';
import { chain } from 'stream-chain';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick';
import { streamArray } from 'stream-json/streamers/StreamArray';
import { Readable } from 'stream';

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
            case "benchmark-batch":
                await this.benchmarkBatchProcessing(message, fileUploadPath);
                break;
            case "benchmark-geojson-stream":
                await this.benchmarkGeoJSONStreaming(message, fileUploadPath);
                break;
            case "benchmark-csv-stream":
                await this.benchmarkCSVStreaming(message, fileUploadPath);
                break;
            case "benchmark-csv-local":
                await this.benchmarkCSVLocal(message, fileUploadPath);
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

                console.time(`processFiles ${tdei_dataset_id}`);
                let processedFiles = 0;
                let totalFeatures = 0;

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.csv') && !entry.path.includes('__MACOSX')) {
                        try {
                            if (entry.path.includes('nodes')) {
                                await this.bulkInsertCSVDirect(client, 'node', entry);
                                processedFiles++;
                            } else if (entry.path.includes('edges')) {
                                await this.bulkInsertCSVDirect(client, 'edge', entry);
                                processedFiles++;
                            } else {
                                entry.autodrain();
                            }
                        } catch (error) {
                            console.error(`Error processing CSV file ${entry.path}:`, error);
                            throw error; // Re-throw to ensure transaction rollback
                        }
                    } else {
                        entry.autodrain();
                    }
                }

                console.log(`Successfully processed ${processedFiles} CSV files`);
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


    public async bulkInsertNodes2(
        client: PoolClient,
        tdei_dataset_id: string,
        user_id: string,
        entry: any
    ): Promise<void> {
        try {
            console.log(`Starting streaming bulk insert for nodes from file: ${entry.path}`);

            // Prepare COPY stream for efficient bulk insert
            const copyStream = client.query(copyFrom(
                `COPY content.node (tdei_dataset_id, feature, requested_by) FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER false)`
            ));

            // Use Node.js pipeline for proper stream handling with unzipper
            const { pipeline } = require('stream');
            const { promisify } = require('util');
            const pipelineAsync = promisify(pipeline);

            // Create a proper streaming pipeline
            const jsonParser = parser();
            const featurePicker = pick({ filter: 'features' });
            const arrayStreamer = streamArray();

            let processedCount = 0;
            const batchSize = 1000;
            let batchBuffer: any[] = [];

            // Handle each feature as it streams
            arrayStreamer.on('data', (data) => {
                batchBuffer.push(data.value);

                // Process batch when it reaches batchSize
                if (batchBuffer.length >= batchSize) {
                    const csvRows = batchBuffer.map((feature: any) => {
                        const featureStr = JSON.stringify(feature)
                            .replace(/"/g, '""')
                            .replace(/\n/g, '\\n')
                            .replace(/\r/g, '\\r');
                        return `${tdei_dataset_id},"${featureStr}",${user_id}`;
                    }).join('\n') + '\n';

                    copyStream.write(csvRows);
                    processedCount += batchBuffer.length;

                    // Log progress
                    if (processedCount % 5000 === 0) {
                        console.log(`Processed ${processedCount} node features`);
                    }

                    batchBuffer = []; // Clear buffer
                }
            });

            arrayStreamer.on('end', () => {
                // Process remaining items in buffer
                if (batchBuffer.length > 0) {
                    const csvRows = batchBuffer.map((feature: any) => {
                        const featureStr = JSON.stringify(feature)
                            .replace(/"/g, '""')
                            .replace(/\n/g, '\\n')
                            .replace(/\r/g, '\\r');
                        return `${tdei_dataset_id},"${featureStr}",${user_id}`;
                    }).join('\n') + '\n';

                    copyStream.write(csvRows);
                    processedCount += batchBuffer.length;
                }

                console.log(`Successfully processed ${processedCount} node features`);
                copyStream.end();
            });

            // Set up error handling
            const handleError = (err: any) => {
                console.error('Stream error in bulkInsertNodes:', err);
                console.error('File path:', entry.path);
                copyStream.destroy();
            };

            jsonParser.on('error', handleError);
            featurePicker.on('error', handleError);
            arrayStreamer.on('error', handleError);

            // Create the pipeline: entry -> parser -> picker -> arrayStreamer
            await pipelineAsync(
                entry,
                jsonParser,
                featurePicker,
                arrayStreamer
            );

            // Wait for COPY to complete
            await new Promise<void>((resolve, reject) => {
                copyStream.on('finish', resolve);
                copyStream.on('error', reject);
            });

        } catch (error) {
            console.error('Error in bulkInsertNodes:', error);
            console.error('File path:', entry.path);
            throw error;
        }
    }

    /**
     * Ultra-fast direct CSV COPY - streams CSV data directly to PostgreSQL COPY
     * CSV file already contains: tdei_dataset_id, feature, requested_by
     * No processing required - maximum performance
     */
    public async bulkInsertCSVDirect(
        client: PoolClient,
        tableName: string,
        entry: any
    ): Promise<void> {
        try {
            // Direct COPY - CSV already has correct format with headers
            const copyStream = client.query(copyFrom(
                `COPY content.${tableName} (tdei_dataset_id, feature, requested_by) FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true)`
            ));

            // Stream CSV data directly to PostgreSQL COPY - zero processing!
            entry.on('data', (chunk: Buffer) => {
                copyStream.write(chunk);
            });

            entry.on('end', () => {
                copyStream.end();
            });

            entry.on('error', (err: any) => {
                console.error(`Error reading CSV stream ${entry.path}:`, err);
                copyStream.destroy();
            });

            // Wait for COPY to complete
            await new Promise<void>((resolve, reject) => {
                copyStream.on('finish', resolve);
                copyStream.on('error', reject);
            });

        } catch (error) {
            console.error(`Error in direct CSV insert for ${tableName}:`, error);
            throw error;
        }
    }

    /**
     * Extract ZIP file to local directory and process CSV files
     * This approach extracts ZIP to local folder first, then reads CSV files
     */
    public async processZIPToLocalCSV(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('Processing ZIP to local CSV dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records for dataset:', tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            // Create local extraction directory
            const fs = require('fs');
            const path = require('path');
            const extractDir = `/tmp/extract_${tdei_dataset_id}_${Date.now()}`;

            // Create extraction directory
            if (!fs.existsSync(extractDir)) {
                fs.mkdirSync(extractDir, { recursive: true });
            }

            try {
                // Extract ZIP file to local directory
                console.log(`Extracting ZIP file to: ${extractDir}`);
                const fileStream = await this.getFileStream(fileUploadPath);
                const directory = fileStream.pipe(unzipper.Parse({ forceStream: true }));

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.csv') && !entry.path.includes('__MACOSX')) {
                        const localFilePath = path.join(extractDir, path.basename(entry.path));
                        const writeStream = fs.createWriteStream(localFilePath);

                        await new Promise<void>((resolve, reject) => {
                            entry.pipe(writeStream)
                                .on('finish', resolve)
                                .on('error', reject);
                        });

                        console.log(`Extracted: ${entry.path} -> ${localFilePath}`);
                    } else {
                        entry.autodrain();
                    }
                }

                // Process extracted CSV files
                await dbClient.runInTransaction(async (client) => {
                    const files = fs.readdirSync(extractDir)
                        .filter((file: string) => file.endsWith('.csv'))
                        .map((file: string) => path.join(extractDir, file));

                    console.time(`processExtractedCSVFiles ${tdei_dataset_id}`);
                    const promises: Promise<void>[] = [];

                    // Process each extracted CSV file
                    for (const csvFilePath of files) {
                        const fileName = path.basename(csvFilePath);

                        if (fileName.includes('nodes')) {
                            promises.push(this.bulkInsertCSVFromLocal(client, 'node', csvFilePath));
                        } else if (fileName.includes('edges')) {
                            promises.push(this.bulkInsertCSVFromLocal(client, 'edge', csvFilePath));
                        } else {
                            console.log(`Skipping unrecognized CSV file: ${fileName}`);
                        }
                    }

                    // Wait for all CSV files to be processed in parallel
                    await Promise.all(promises);
                    console.timeEnd(`processExtractedCSVFiles ${tdei_dataset_id}`);
                });

                console.log(`ZIP to local CSV data loaded successfully for dataset: ${tdei_dataset_id}`);
                await this.publishMessage(message, true, "ZIP to local CSV data loaded successfully");

            } finally {
                // Clean up extracted files
                console.log(`Cleaning up extracted files from: ${extractDir}`);
                try {
                    const files = fs.readdirSync(extractDir);
                    for (const file of files) {
                        fs.unlinkSync(path.join(extractDir, file));
                    }
                    fs.rmdirSync(extractDir);
                } catch (cleanupError) {
                    console.error('Error cleaning up extracted files:', cleanupError);
                }
            }

        } catch (error) {
            console.error('Error processing ZIP to local CSV:', error);
            await this.publishMessage(message, false, "Error processing ZIP to local CSV: " + error);
        }
    }

    /**
     * Ultra-fast local CSV file loading using PostgreSQL COPY
     * Reads CSV files directly from local filesystem
     */
    public async bulkInsertCSVFromLocal(
        client: PoolClient,
        tableName: string,
        csvFilePath: string
    ): Promise<void> {
        try {
            const fs = require('fs');

            // Check if file exists
            if (!fs.existsSync(csvFilePath)) {
                throw new Error(`CSV file not found: ${csvFilePath}`);
            }

            // Direct COPY - CSV already has correct format with headers
            const copyStream = client.query(copyFrom(
                `COPY content.${tableName} (tdei_dataset_id, feature, requested_by) FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true)`
            ));

            // Create read stream from local file
            const fileStream = fs.createReadStream(csvFilePath);

            // Stream CSV data directly to PostgreSQL COPY - zero processing!
            fileStream.on('data', (chunk: Buffer) => {
                copyStream.write(chunk);
            });

            fileStream.on('end', () => {
                copyStream.end();
            });

            fileStream.on('error', (err: any) => {
                console.error(`Error reading local CSV file ${csvFilePath}:`, err);
                copyStream.destroy();
            });

            // Wait for COPY to complete
            await new Promise<void>((resolve, reject) => {
                copyStream.on('finish', resolve);
                copyStream.on('error', reject);
            });

        } catch (error) {
            console.error(`Error in local CSV insert for ${tableName}:`, error);
            throw error;
        }
    }

    // ========================================
    // BENCHMARK METHODS - 4 Different Approaches
    // ========================================

    /**
     * BENCHMARK 1: Original Batch Processing Approach
     * Reads ZIP file, extracts GeoJSON, parses JSON, processes batch by batch
     */
    public async benchmarkBatchProcessing(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('🚀 BENCHMARK 1: Starting Batch Processing for dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records for dataset:', tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            await dbClient.runInTransaction(async (client) => {
                const directory = (await this.getFileStream(fileUploadPath)).pipe(unzipper.Parse({ forceStream: true }));

                console.time(`BENCHMARK-1-BatchProcessing ${tdei_dataset_id}`);
                let processedFiles = 0;

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.geojson') && !entry.path.includes('__MACOSX')) {
                        try {
                            console.log(`Processing file: ${entry.path}`);

                            // Read entire file into memory and parse JSON
                            const content = await entry.buffer();
                            const jsonData = JSON.parse(content.toString('utf8'));

                            if (entry.path.includes('nodes')) {
                                await this.bulkInsertNodes(client, tdei_dataset_id, user_id, jsonData);
                                processedFiles++;
                            } else if (entry.path.includes('edges')) {
                                await this.bulkInsertEdges(client, tdei_dataset_id, user_id, jsonData);
                                processedFiles++;
                            } else {
                                entry.autodrain();
                            }
                        } catch (error) {
                            console.error(`Error processing file ${entry.path}:`, error);
                            throw error;
                        }
                    } else {
                        entry.autodrain();
                    }
                }

                console.log(`Successfully processed ${processedFiles} GeoJSON files`);
                console.timeEnd(`BENCHMARK-1-BatchProcessing ${tdei_dataset_id}`);
            });

            console.log(`✅ BENCHMARK 1: Batch processing completed for dataset: ${tdei_dataset_id}`);
            await this.publishMessage(message, true, "BENCHMARK 1: Batch processing completed successfully");

        } catch (error) {
            console.error('❌ BENCHMARK 1: Error in batch processing:', error);
            await this.publishMessage(message, false, "BENCHMARK 1: Error in batch processing: " + error);
        }
    }

    /**
     * BENCHMARK 2: GeoJSON Streaming with COPY
     * Streams GeoJSON files and uses PostgreSQL COPY for bulk insert
     */
    public async benchmarkGeoJSONStreaming(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('🚀 BENCHMARK 2: Starting GeoJSON Streaming for dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records for dataset:', tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            await dbClient.runInTransaction(async (client) => {
                const directory = (await this.getFileStream(fileUploadPath)).pipe(unzipper.Parse({ forceStream: true }));

                console.time(`BENCHMARK-2-GeoJSONStreaming ${tdei_dataset_id}`);
                let processedFiles = 0;

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.geojson') && !entry.path.includes('__MACOSX')) {
                        try {
                            console.log(`Processing file: ${entry.path}`);

                            if (entry.path.includes('nodes')) {
                                await this.bulkInsertNodes2(client, tdei_dataset_id, user_id, entry);
                                processedFiles++;
                            } else if (entry.path.includes('edges')) {
                                // For edges, we need to use the original method since bulkInsertEdges2 doesn't exist
                                const content = await entry.buffer();
                                const jsonData = JSON.parse(content.toString('utf8'));
                                await this.bulkInsertEdges(client, tdei_dataset_id, user_id, jsonData);
                                processedFiles++;
                            } else {
                                entry.autodrain();
                            }
                        } catch (error) {
                            console.error(`Error processing file ${entry.path}:`, error);
                            throw error;
                        }
                    } else {
                        entry.autodrain();
                    }
                }

                console.log(`Successfully processed ${processedFiles} GeoJSON files`);
                console.timeEnd(`BENCHMARK-2-GeoJSONStreaming ${tdei_dataset_id}`);
            });

            console.log(`✅ BENCHMARK 2: GeoJSON streaming completed for dataset: ${tdei_dataset_id}`);
            await this.publishMessage(message, true, "BENCHMARK 2: GeoJSON streaming completed successfully");

        } catch (error) {
            console.error('❌ BENCHMARK 2: Error in GeoJSON streaming:', error);
            await this.publishMessage(message, false, "BENCHMARK 2: Error in GeoJSON streaming: " + error);
        }
    }

    /**
     * BENCHMARK 3: CSV Streaming from ZIP
     * Streams CSV files directly from ZIP using PostgreSQL COPY
     */
    public async benchmarkCSVStreaming(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('🚀 BENCHMARK 3: Starting CSV Streaming for dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records for dataset:', tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            await dbClient.runInTransaction(async (client) => {
                const directory = (await this.getFileStream(fileUploadPath)).pipe(unzipper.Parse({ forceStream: true }));

                console.time(`BENCHMARK-3-CSVStreaming ${tdei_dataset_id}`);
                let processedFiles = 0;

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.csv') && !entry.path.includes('__MACOSX')) {
                        try {
                            console.log(`Processing CSV file: ${entry.path}`);

                            if (entry.path.includes('nodes')) {
                                await this.bulkInsertCSVDirect(client, 'node', entry);
                                processedFiles++;
                            } else if (entry.path.includes('edges')) {
                                await this.bulkInsertCSVDirect(client, 'edge', entry);
                                processedFiles++;
                            } else {
                                entry.autodrain();
                            }
                        } catch (error) {
                            console.error(`Error processing CSV file ${entry.path}:`, error);
                            throw error;
                        }
                    } else {
                        entry.autodrain();
                    }
                }

                console.log(`Successfully processed ${processedFiles} CSV files`);
                console.timeEnd(`BENCHMARK-3-CSVStreaming ${tdei_dataset_id}`);
            });

            console.log(`✅ BENCHMARK 3: CSV streaming completed for dataset: ${tdei_dataset_id}`);
            await this.publishMessage(message, true, "BENCHMARK 3: CSV streaming completed successfully");

        } catch (error) {
            console.error('❌ BENCHMARK 3: Error in CSV streaming:', error);
            await this.publishMessage(message, false, "BENCHMARK 3: Error in CSV streaming: " + error);
        }
    }

    /**
     * BENCHMARK 4: CSV Local Processing
     * Downloads ZIP to local, extracts CSV files, then processes locally
     */
    public async benchmarkCSVLocal(message: QueueMessage, fileUploadPath: string) {
        const tdei_dataset_id = message.data.tdei_dataset_id;
        const user_id = message.data.user_id;
        console.log('🚀 BENCHMARK 4: Starting CSV Local Processing for dataset:', tdei_dataset_id);

        try {
            console.log('Deleting existing records for dataset:', tdei_dataset_id);
            const deleteRecordsQueryObject = {
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: [tdei_dataset_id]
            };
            await dbClient.query(deleteRecordsQueryObject);

            // Create local extraction directory
            const fs = require('fs');
            const path = require('path');
            const extractDir = `/tmp/benchmark_extract_${tdei_dataset_id}_${Date.now()}`;

            // Create extraction directory
            if (!fs.existsSync(extractDir)) {
                fs.mkdirSync(extractDir, { recursive: true });
            }

            try {
                // Extract ZIP file to local directory
                console.log(`Extracting ZIP file to: ${extractDir}`);
                const fileStream = await this.getFileStream(fileUploadPath);
                const directory = fileStream.pipe(unzipper.Parse({ forceStream: true }));

                for await (const entry of directory) {
                    if (entry.type === 'File' && entry.path.endsWith('.csv') && !entry.path.includes('__MACOSX')) {
                        const localFilePath = path.join(extractDir, path.basename(entry.path));
                        const writeStream = fs.createWriteStream(localFilePath);

                        await new Promise<void>((resolve, reject) => {
                            entry.pipe(writeStream)
                                .on('finish', resolve)
                                .on('error', reject);
                        });

                        console.log(`Extracted: ${entry.path} -> ${localFilePath}`);
                    } else {
                        entry.autodrain();
                    }
                }

                // Process extracted CSV files
                await dbClient.runInTransaction(async (client) => {
                    const files = fs.readdirSync(extractDir)
                        .filter((file: string) => file.endsWith('.csv'))
                        .map((file: string) => path.join(extractDir, file));

                    console.time(`BENCHMARK-4-CSVLocal ${tdei_dataset_id}`);
                    const promises: Promise<void>[] = [];

                    // Process each extracted CSV file
                    for (const csvFilePath of files) {
                        const fileName = path.basename(csvFilePath);

                        if (fileName.includes('nodes')) {
                            promises.push(this.bulkInsertCSVFromLocal(client, 'node', csvFilePath));
                        } else if (fileName.includes('edges')) {
                            promises.push(this.bulkInsertCSVFromLocal(client, 'edge', csvFilePath));
                        } else {
                            console.log(`Skipping unrecognized CSV file: ${fileName}`);
                        }
                    }

                    // Wait for all CSV files to be processed in parallel
                    await Promise.all(promises);
                    console.timeEnd(`BENCHMARK-4-CSVLocal ${tdei_dataset_id}`);
                });

                console.log(`✅ BENCHMARK 4: CSV local processing completed for dataset: ${tdei_dataset_id}`);
                await this.publishMessage(message, true, "BENCHMARK 4: CSV local processing completed successfully");

            } finally {
                // Clean up extracted files
                console.log(`Cleaning up extracted files from: ${extractDir}`);
                try {
                    const files = fs.readdirSync(extractDir);
                    for (const file of files) {
                        fs.unlinkSync(path.join(extractDir, file));
                    }
                    fs.rmdirSync(extractDir);
                } catch (cleanupError) {
                    console.error('Error cleaning up extracted files:', cleanupError);
                }
            }

        } catch (error) {
            console.error('❌ BENCHMARK 4: Error in CSV local processing:', error);
            await this.publishMessage(message, false, "BENCHMARK 4: Error in CSV local processing: " + error);
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
            console.time(`bulkInsertEdges ${tdei_dataset_id}`);
            console.log('Inserting edge records');
            // Batch processing
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
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
            while (jsonData.features.length > 0) {
                const batch = jsonData.features.splice(0, batchSize);
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


