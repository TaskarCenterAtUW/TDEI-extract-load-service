import { ExtractLoadService } from "../../src/service/extract-load-service";
import { mockCore } from "../common/mock-utils";
import dbClient from "../../src/database/data-source";
import { Readable } from 'stream';
import unzipper from 'unzipper';

jest.mock('unzipper', () => ({
    Parse: jest.fn()
}));

describe('ExtractLoadService', () => {
    let extractLoadService: ExtractLoadService;

    beforeEach(() => {
        extractLoadService = new ExtractLoadService();
    });


    describe('extract Load Request Processor', () => {
        it('should process OSW dataset when data_type is "osw"', async () => {
            // Arrange
            const message: any = {
                data: {
                    file_upload_path: "path/to/osw/file",
                    data_type: "osw"
                }
            };

            const processOSWDatasetMock = jest.spyOn(extractLoadService, 'processOSWDataset').mockResolvedValueOnce();
            mockCore();
            // Act
            const result = await extractLoadService.extractLoadRequestProcessor(message);

            // Assert
            expect(processOSWDatasetMock).toHaveBeenCalledWith(message, expect.any(String));
            expect(result).toBe(true);
        });

        it('should process Flex dataset when data_type is "flex"', async () => {
            // Arrange
            const message: any = {
                data: {
                    file_upload_path: "path/to/flex/file",
                    data_type: "flex"
                }
            };

            const processFlexDatasetMock = jest.spyOn(extractLoadService, 'processFlexDataset').mockResolvedValueOnce();
            mockCore();

            // Act
            const result = await extractLoadService.extractLoadRequestProcessor(message);

            // Assert
            expect(processFlexDatasetMock).toHaveBeenCalledWith(message, expect.any(String));
            expect(result).toBe(true);
        });

        it('should process Pathways dataset when data_type is "pathways"', async () => {
            // Arrange
            const message: any = {
                data: {
                    file_upload_path: "path/to/pathways/file",
                    data_type: "pathways"
                }
            };

            const processPathwaysDatasetMock = jest.spyOn(extractLoadService, 'processPathwaysDataset').mockResolvedValueOnce();
            mockCore();

            // Act
            const result = await extractLoadService.extractLoadRequestProcessor(message);

            // Assert
            expect(processPathwaysDatasetMock).toHaveBeenCalledWith(message, expect.any(String));
            expect(result).toBe(true);
        });

        // Successfully delete existing records before inserting new ones
        it('should delete existing records and insert new ones when successful', async () => {
            // Arrange
            const message: any = {
                data: {
                    tdei_dataset_id: "dataset123",
                    user_id: "user123"
                }
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
            const runInTransactionSpy = jest.spyOn(dbClient, 'runInTransaction')
                .mockImplementation(async (callback) => {
                    await callback({} as any);
                });

            const publishMessageMock = jest.fn();

            extractLoadService.publishMessage = publishMessageMock;
            extractLoadService.bulkInsertNodes = jest.fn();
            extractLoadService.bulkInsertEdges = jest.fn();
            extractLoadService.bulkInsertPoints = jest.fn();
            extractLoadService.bulkInsertLines = jest.fn();
            extractLoadService.bulkInsertPolygons = jest.fn();
            extractLoadService.bulkInsertZones = jest.fn();


            // Mock getFileStream to return a readable stream
            const mockFileStream = new Readable();
            mockFileStream._read = () => { }; // No-op
            jest.spyOn(extractLoadService, 'getFileStream').mockResolvedValue(mockFileStream);

            // Mock unzipper.Parse to return a mock stream
            const mockUnzipStream = new Readable({ objectMode: true });
            mockUnzipStream._read = () => { }; // No-op
            (unzipper.Parse as jest.Mock).mockReturnValue(mockUnzipStream);

            // Push the mock entry to the unzip stream
            mockUnzipStream.push(getMockEntry("edges.geojson"));
            mockUnzipStream.push(getMockEntry("nodes.geojson"));
            mockUnzipStream.push(getMockEntry("points.geojson"));
            mockUnzipStream.push(getMockEntry("lines.geojson"));
            mockUnzipStream.push(getMockEntry("polygons.geojson"));
            mockUnzipStream.push(getMockEntry("zones.geojson"));
            //Add more entries with different file names

            mockUnzipStream.push(null); // End the stream

            // Act
            await extractLoadService.processOSWDataset(message, "file_upload_path");

            // Assert
            expect(querySpy).toHaveBeenCalledWith({
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: ["dataset123"]
            });

            expect(runInTransactionSpy).toHaveBeenCalled();

            expect(extractLoadService.bulkInsertNodes).toHaveBeenCalled();
            expect(extractLoadService.bulkInsertEdges).toHaveBeenCalled();
            expect(extractLoadService.bulkInsertPoints).toHaveBeenCalled();
            expect(extractLoadService.bulkInsertLines).toHaveBeenCalled();
            expect(extractLoadService.bulkInsertPolygons).toHaveBeenCalled();

            expect(publishMessageMock).toHaveBeenCalledWith(message, true, "Data loaded successfully");

            function getMockEntry(name: string) {
                const mockEntry: any = new Readable({ objectMode: true });
                mockEntry._read = () => { }; // No-op
                mockEntry.type = 'File';
                mockEntry.path = name;
                mockEntry.buffer = jest.fn().mockResolvedValue(Buffer.from(JSON.stringify({ features: [] })));
                return mockEntry;
            }
        }, 15000);
        it('should publish message if any error occurs', async () => {
            // Arrange
            const message: any = {
                data: {
                    tdei_dataset_id: "dataset123",
                    user_id: "user123"
                }
            };

            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Error'));
            const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
            const publishMessageMock = jest.fn();
            extractLoadService.publishMessage = publishMessageMock;
            // Mock getFileStream to return a readable stream
            const mockFileStream = new Readable();
            mockFileStream._read = () => { }; // No-op
            jest.spyOn(extractLoadService, 'getFileStream').mockResolvedValue(mockFileStream);
            // Act
            await extractLoadService.processOSWDataset(message, "file_upload_path");

            // Assert
            expect(querySpy).toHaveBeenCalledWith({
                text: `SELECT delete_dataset_records_by_id($1)`.replace(/\n/g, ""),
                values: ["dataset123"]
            });
            expect(publishMessageMock).toHaveBeenCalledWith(message, false, expect.any(String));
        });
    });
    describe('bulk Insert Edges', () => {
        it('should insert edges in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Edge 1' },
                    { id: 2, name: 'Edge 2' },
                    { id: 3, name: 'Edge 3' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertEdges({} as any, tdei_dataset_id, user_id, jsonData);

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting edge records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Edge 1' },
                    { id: 2, name: 'Edge 2' },
                    { id: 3, name: 'Edge 3' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertEdges({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });

    describe('bulk Insert Nodes', () => {
        it('should insert nodes in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Node 1' },
                    { id: 2, name: 'Node 2' },
                    { id: 3, name: 'Node 3' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertNodes({} as any, tdei_dataset_id, user_id, jsonData);

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting node records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Node 1' },
                    { id: 2, name: 'Node 2' },
                    { id: 3, name: 'Node 3' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertNodes({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });

    describe('bulk Insert Points', () => {
        it('should insert points in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Point 1' },
                    { id: 2, name: 'Point 2' },
                    { id: 3, name: 'Point 3' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertPoints({} as any, tdei_dataset_id, user_id, jsonData);

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting point records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Point 1' },
                    { id: 2, name: 'Point 2' },
                    { id: 3, name: 'Point 3' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertPoints({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });

    describe('bulk Insert Polygons', () => {
        it('should insert polygons in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Polygons 1' },
                    { id: 2, name: 'Polygons 2' },
                    { id: 3, name: 'Polygons 3' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertPolygons({} as any, tdei_dataset_id, user_id, jsonData);

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting polygons records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Polygons 1' },
                    { id: 2, name: 'Polygons 2' },
                    { id: 3, name: 'Polygons 3' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertPolygons({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });

    describe('bulk Insert Lines', () => {
        it('should insert lines in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Lines 1' },
                    { id: 2, name: 'Lines 2' },
                    { id: 3, name: 'Lines 3' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce(undefined);
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertLines({} as any, tdei_dataset_id, user_id, jsonData);

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting lines records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Lines 1' },
                    { id: 2, name: 'Lines 2' },
                    { id: 3, name: 'Lines 3' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertLines({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });

    describe('bulk Insert Extension file', () => {
        it('should insert ext geom in batches', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Lines 1' },
                    { id: 2, name: 'Polygon' },
                    { id: 3, name: 'Point' },
                ],
            };

            const queryMock = jest.fn().mockResolvedValueOnce({
                rows: [
                    { id: 1 }
                ]
            });
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            // Act
            await extractLoadService.bulkInsertExtension({} as any, tdei_dataset_id, user_id, jsonData, { path: "" });

            // Assert
            expect(querySpy).toHaveBeenCalled();
        });

        it('should throw an error if there is an error inserting lines records', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    { id: 1, name: 'Lines 1' },
                    { id: 2, name: 'Polygon' },
                    { id: 3, name: 'Point' },
                ],
            };
            const queryMock = jest.fn().mockRejectedValueOnce(new Error('Database error'));
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertExtension({} as any, tdei_dataset_id, user_id, jsonData, { path: "" })
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });
});