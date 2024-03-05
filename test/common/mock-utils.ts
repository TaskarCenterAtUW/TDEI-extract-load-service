import AdmZip from "adm-zip";
import { Core } from "nodets-ms-core"
import { IAuthorizer } from "nodets-ms-core/lib/core/auth/abstracts/IAuthorizer";
import { Topic } from "nodets-ms-core/lib/core/queue/topic";
import { FileEntity, StorageClient, StorageContainer } from "nodets-ms-core/lib/core/storage"
import { Readable } from "stream"

export function getMockZipFileStream() {
    // Create a new AdmZip instance
    const zip = new AdmZip();

    // Add files to the zip
    zip.addFile('edges.geojson', Buffer.from('{ "features": [ { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "LineString", "coordinates": [ [ 0, 0 ], [ 1, 1 ] ] } } ], "type": "FeatureCollection"}'));
    zip.addFile('nodes.geojson', Buffer.from('{ "features": [ { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "Point", "coordinates": [ 0, 0 ] } } ], "type": "FeatureCollection"}'));
    zip.addFile('points.geojson', Buffer.from('{ "features": [ { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "Point", "coordinates": [ 0, 0 ] } } ], "type": "FeatureCollection"}'));
    zip.addFile('lines.geojson', Buffer.from('{ "features": [ { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "LineString", "coordinates": [ [ 0, 0 ], [ 1, 1 ] ] } } ], "type": "FeatureCollection"}'));
    zip.addFile('polygons.geojson', Buffer.from('{ "features": [ { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 0, 0 ], [ 1, 1 ], [ 1, 0 ], [ 0, 0 ] ] ] } } ], "type": "FeatureCollection"}'));

    // Get the zip file as a buffer
    const zipBuffer = zip.toBuffer();

    // Create a new readable stream from the buffer
    let mockedStream = new Readable();
    mockedStream.push(zipBuffer);
    mockedStream.push(null); // Indicates end of file
    return mockedStream;
}
export function getMockFileEntity() {
    const fileEntity: FileEntity = {
        fileName: "test_file_name",
        mimeType: "csv",
        filePath: "test_file_path",
        remoteUrl: "",
        getStream: function (): Promise<NodeJS.ReadableStream> {
            const mockedStream = new Readable();
            mockedStream._read = function () { /* do nothing */ };
            return Promise.resolve(mockedStream);
        },
        getBodyText: function (): Promise<string> {
            return Promise.resolve("Sample body test");
        },
        upload: function (): Promise<FileEntity> {
            return Promise.resolve(this);
        }
    };
    return fileEntity;
}

export function getMockStorageClient() {
    const storageClientObj: StorageClient = {
        getContainer: function (): Promise<StorageContainer> {
            return Promise.resolve(getMockStorageContainer());
        },
        getFile: function (): Promise<FileEntity> {
            return Promise.resolve(getMockFileEntity());
        },
        getFileFromUrl: function (): Promise<FileEntity> {
            return Promise.resolve(getMockFileEntity());
        }
    };
    return storageClientObj;
}

export function getMockStorageContainer() {
    const storageContainerObj: StorageContainer = {
        name: "test_container",
        listFiles: function (): Promise<FileEntity[]> {
            return Promise.resolve([getMockFileEntity()]);
        },
        createFile: function (): FileEntity {
            return getMockFileEntity();
        }
    };
    return storageContainerObj;
}

export function getMockTopic() {
    const mockTopic: Topic = new Topic({ provider: "Azure" }, "test");
    mockTopic.publish = (): Promise<void> => {
        return Promise.resolve();
    }

    return mockTopic;
}


export function getMockAuthorizer(result: boolean) {
    const authorizor: IAuthorizer = {
        hasPermission(permissionRequest) {
            return Promise.resolve(result);
        },
    }
    return authorizor;
}

export function mockCoreAuth(result: boolean) {
    jest.spyOn(Core, 'getAuthorizer').mockImplementation(() => { return getMockAuthorizer(result); })

}


export function mockCore() {
    jest.spyOn(Core, "initialize");
    jest.spyOn(Core, "getStorageClient").mockImplementation(() => { return getMockStorageClient(); });
    jest.spyOn(Core, "getTopic").mockImplementation(() => { return getMockTopic(); });


}
