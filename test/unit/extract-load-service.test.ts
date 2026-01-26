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

        it('should strip Z coordinates from mixed geometry types in extension files', async () => {
            // Arrange
            const tdei_dataset_id = 'dataset123';
            const user_id = 'user123';
            const jsonData = {
                features: [
                    {
                        type: 'Feature',
                        geometry: {
                            type: 'Point',
                            coordinates: [-122.1, 47.6, 100.0] // 3D Point
                        },
                        properties: { id: 1, name: 'Point Feature' }
                    },
                    {
                        type: 'Feature',
                        geometry: {
                            type: 'LineString',
                            coordinates: [
                                [-122.2, 47.7, 200.0],
                                [-122.3, 47.8, 300.0]
                            ] // 3D LineString
                        },
                        properties: { id: 2, name: 'LineString Feature' }
                    },
                    {
                        type: 'Feature',
                        geometry: {
                            type: 'Polygon',
                            coordinates: [
                                [
                                    [-122.4, 47.9, 400.0],
                                    [-122.5, 48.0, 500.0],
                                    [-122.4, 47.9, 400.0]
                                ]
                            ] // 3D Polygon
                        },
                        properties: { id: 3, name: 'Polygon Feature' }
                    }
                ],
            };

            const queryMock = jest.fn().mockResolvedValue({
                rows: [{ id: 1 }]
            });
            const querySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);

            const extractLoadService = new ExtractLoadService();
            extractLoadService.updateAdditionalFileData = jest.fn();
            extractLoadService.updateExtensionFileData = jest.fn().mockResolvedValue(1);

            // Act
            await extractLoadService.bulkInsertExtension({} as any, tdei_dataset_id, user_id, jsonData, { path: "test.geojson" });

            // Assert - Check that Z coordinates are stripped from all geometries
            const queryObject = querySpy.mock.calls[0][1] as any;
            const insertedFeatures = queryObject.values;

            // Point feature (index 2 in values array: tdei_dataset_id, ext_file_id, record, user_id)
            const pointFeature = insertedFeatures[2];
            expect(pointFeature.geometry.coordinates).toEqual([-122.1, 47.6]); // Z stripped
            expect(pointFeature.properties['ext:elevation']).toBeUndefined(); // No elevation property

            // LineString feature
            const lineFeature = insertedFeatures[6];
            expect(lineFeature.geometry.coordinates).toEqual([
                [-122.2, 47.7],
                [-122.3, 47.8]
            ]); // Z stripped
            expect(lineFeature.properties['ext:elevation']).toBeUndefined();

            // Polygon feature
            const polygonFeature = insertedFeatures[10];
            expect(polygonFeature.geometry.coordinates).toEqual([
                [
                    [-122.4, 47.9],
                    [-122.5, 48.0],
                    [-122.4, 47.9]
                ]
            ]); // Z stripped
            expect(polygonFeature.properties['ext:elevation']).toBeUndefined();
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

    describe('Geometry Elevation Processing', () => {
        let extractLoadService: ExtractLoadService;
        let mockClient: any;
        let executeQuerySpy: jest.SpyInstance;

        beforeEach(() => {
            extractLoadService = new ExtractLoadService();
            mockClient = {} as any;
            const queryMock = jest.fn().mockResolvedValue(undefined);
            executeQuerySpy = jest.spyOn(dbClient, 'executeQuery').mockImplementation(queryMock);
            extractLoadService.updateAdditionalFileData = jest.fn().mockResolvedValue(undefined);
        });

        afterEach(() => {
            jest.clearAllMocks();
        });

        describe('Nodes - Z coordinate stripping and elevation extraction', () => {
            it('should extract elevation and strip Z from 3D Point geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [-122.1355703, 47.6458165, 123.45] // 3D Point
                            },
                            properties: { _id: 'node1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([-122.1355703, 47.6458165]); // Z stripped
                expect(insertedFeature.properties['ext:elevation']).toBe(123.45);
            });

            it('should handle existing ext:elevation property and use ext:elevation_1', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [-122.1, 47.6, 150.0]
                            },
                            properties: {
                                _id: 'node3',
                                'ext:elevation': 100.0 // Existing property
                            }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.properties['ext:elevation']).toBe(100.0); // Original preserved
                expect(insertedFeature.properties['ext:elevation_1']).toBe(150.0); // New elevation
            });

            it('should handle multiple existing ext:elevation properties', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [-122.1, 47.6, 200.0]
                            },
                            properties: {
                                _id: 'node4',
                                'ext:elevation': 100.0,
                                'ext:elevation_1': 150.0
                            }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.properties['ext:elevation']).toBe(100.0);
                expect(insertedFeature.properties['ext:elevation_1']).toBe(150.0);
                expect(insertedFeature.properties['ext:elevation_2']).toBe(200.0);
            });

            it('should handle 2D Point geometry without elevation', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [-122.1355703, 47.6458165] // 2D Point
                            },
                            properties: { _id: 'node5' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([-122.1355703, 47.6458165]);
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });
        });

        describe('Points - Z coordinate stripping and elevation extraction', () => {
            it('should extract elevation and strip Z from 3D Point geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [-122.1, 47.6, 250.0]
                            },
                            properties: { _id: 'point1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertPoints(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([-122.1, 47.6]); // Z stripped
                expect(insertedFeature.properties['ext:elevation']).toBe(250.0);
            });
        });

        describe('Edges - Z coordinate stripping only', () => {
            it('should strip Z from 3D LineString geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'LineString',
                                coordinates: [
                                    [-122.1, 47.6, 100.0],
                                    [-122.2, 47.7, 200.0],
                                    [-122.3, 47.8, 300.0]
                                ]
                            },
                            properties: { _id: 'edge1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertEdges(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [-122.1, 47.6],
                    [-122.2, 47.7],
                    [-122.3, 47.8]
                ]); // Z stripped from all coordinates
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });

            it('should handle 2D LineString geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'LineString',
                                coordinates: [
                                    [-122.1, 47.6],
                                    [-122.2, 47.7]
                                ]
                            },
                            properties: { _id: 'edge2' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertEdges(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [-122.1, 47.6],
                    [-122.2, 47.7]
                ]);
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });
        });

        describe('Lines - Z coordinate stripping only', () => {
            it('should strip Z from 3D LineString geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'LineString',
                                coordinates: [
                                    [-122.1, 47.6, 150.0],
                                    [-122.2, 47.7, 160.0]
                                ]
                            },
                            properties: { _id: 'line1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertLines(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [-122.1, 47.6],
                    [-122.2, 47.7]
                ]); // Z stripped
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });

            it('should strip Z from 3D MultiLineString geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'MultiLineString',
                                coordinates: [
                                    [
                                        [-122.1, 47.6, 100.0],
                                        [-122.2, 47.7, 110.0]
                                    ],
                                    [
                                        [-122.3, 47.8, 120.0],
                                        [-122.4, 47.9, 130.0]
                                    ]
                                ]
                            },
                            properties: { _id: 'line2' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertLines(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [
                        [-122.1, 47.6],
                        [-122.2, 47.7]
                    ],
                    [
                        [-122.3, 47.8],
                        [-122.4, 47.9]
                    ]
                ]); // Z stripped from all coordinates
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });
        });

        describe('Polygons - Z coordinate stripping only', () => {
            it('should strip Z from 3D Polygon geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Polygon',
                                coordinates: [
                                    [
                                        [-122.1, 47.6, 100.0],
                                        [-122.2, 47.7, 110.0],
                                        [-122.3, 47.8, 120.0],
                                        [-122.1, 47.6, 100.0]
                                    ]
                                ]
                            },
                            properties: { _id: 'polygon1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertPolygons(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [
                        [-122.1, 47.6],
                        [-122.2, 47.7],
                        [-122.3, 47.8],
                        [-122.1, 47.6]
                    ]
                ]); // Z stripped from all coordinates
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });

            it('should strip Z from 3D MultiPolygon geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'MultiPolygon',
                                coordinates: [
                                    [
                                        [
                                            [-122.1, 47.6, 100.0],
                                            [-122.2, 47.7, 110.0],
                                            [-122.1, 47.6, 100.0]
                                        ]
                                    ],
                                    [
                                        [
                                            [-122.3, 47.8, 120.0],
                                            [-122.4, 47.9, 130.0],
                                            [-122.3, 47.8, 120.0]
                                        ]
                                    ]
                                ]
                            },
                            properties: { _id: 'polygon2' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertPolygons(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [
                        [
                            [-122.1, 47.6],
                            [-122.2, 47.7],
                            [-122.1, 47.6]
                        ]
                    ],
                    [
                        [
                            [-122.3, 47.8],
                            [-122.4, 47.9],
                            [-122.3, 47.8]
                        ]
                    ]
                ]); // Z stripped from all coordinates
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });
        });

        describe('Zones - Z coordinate stripping only', () => {
            it('should strip Z from 3D Polygon geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Polygon',
                                coordinates: [
                                    [
                                        [-122.1, 47.6, 200.0],
                                        [-122.2, 47.7, 210.0],
                                        [-122.1, 47.6, 200.0]
                                    ]
                                ]
                            },
                            properties: { _id: 'zone1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertZones(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [
                        [-122.1, 47.6],
                        [-122.2, 47.7],
                        [-122.1, 47.6]
                    ]
                ]); // Z stripped from all coordinates
                expect(insertedFeature.properties['ext:elevation']).toBeUndefined();
            });
        });

        describe('Complex scenarios', () => {
            it('should handle mixed 2D and 3D coordinates in LineString', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'LineString',
                                coordinates: [
                                    [-122.1, 47.6], // 2D
                                    [-122.2, 47.7, 100.0], // 3D
                                    [-122.3, 47.8] // 2D
                                ]
                            },
                            properties: { _id: 'edge3' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertEdges(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toEqual([
                    [-122.1, 47.6],
                    [-122.2, 47.7],
                    [-122.3, 47.8]
                ]); // All Z stripped
            });

            it('should handle feature without geometry', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            properties: { _id: 'feature1' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry).toBeUndefined();
            });

            it('should handle feature with null coordinates', async () => {
                const jsonData = {
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: null
                            },
                            properties: { _id: 'feature2' }
                        }
                    ]
                };

                await extractLoadService.bulkInsertNodes(mockClient, 'dataset123', 'user123', jsonData);

                const insertedFeature = executeQuerySpy.mock.calls[0][1].values[1];
                expect(insertedFeature.geometry.coordinates).toBeNull();
            });
        });
    });
});