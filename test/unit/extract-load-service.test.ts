import { ExtractLoadService } from "../../src/service/extract-load-service";
import { getMockZipFileStream } from "../common/mock-utils";
import dbClient from "../../src/database/data-source";


describe('BackendService', () => {
    let extractLoadService: ExtractLoadService;

    beforeEach(() => {
        extractLoadService = new ExtractLoadService();
    });

    describe('extract Load Request Processor', () => {
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

            // Act
            await extractLoadService.processOSWDataset(message, getMockZipFileStream());

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
        });
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

            // Act
            await extractLoadService.processOSWDataset(message, getMockZipFileStream());

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

            // Act & Assert
            await expect(
                extractLoadService.bulkInsertLines({} as any, tdei_dataset_id, user_id, jsonData)
            ).rejects.toThrow();

            expect(querySpy).toHaveBeenCalled();

        });
    });
});