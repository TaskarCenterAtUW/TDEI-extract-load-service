import { Core } from "nodets-ms-core";
import { PermissionRequest } from "nodets-ms-core/lib/core/auth/model/permission_request";
import { ExtractLoadService } from "../../src/service/extract-load-service";
import { mockCore } from "../common/mock-utils";


describe('extractLoadRequestProcessor', () => {
    // Extracts and loads data successfully when given valid inputs and authorized user.
    test('should extract and load data successfully when given valid inputs and authorized user', async () => {
        // Arrange
        const message: any = {
            messageId: "12345",
            data: {
                tdei_project_group_id: "project123",
                data_type: "osw",
                file_upload_path: "/path/to/file",
                user_id: "user123"
            }
        };
        const extractLoadService = new ExtractLoadService();
        const processOswDatasetMock = jest.spyOn(extractLoadService, 'processOSWDataset').mockResolvedValue(undefined);
        const authProviderMock = {
            hasPermission: jest.fn().mockResolvedValue(true)
        };
        jest.spyOn(Core, 'getAuthorizer').mockReturnValue(authProviderMock);
        mockCore();
        // Act
        const result = await extractLoadService.extractLoadRequestProcessor(message);

        // Assert
        expect(result).toBe(true);
        expect(authProviderMock.hasPermission).toHaveBeenCalledWith(expect.any(PermissionRequest));
        expect(processOswDatasetMock).toHaveBeenCalled();
    }, 15000);
});

