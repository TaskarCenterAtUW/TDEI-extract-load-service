# TDEI-extract-load-service
TDEI extract load service to unzip the dataset files and load the records to the database.

## Message Extract Load Request 

```json
{
    "messageId": "tdei_dataset_id",
    "messageType": "workflow_identifier",
    "data": {
      "data_type":"osw|flex|pathways",
      "file_upload_path" : "file path",
      "user_id": "user_id",
      "tdei_project_group_id": "tdei_project_group_id"
    } 
  }
```

## Message Extract Load Response 

```json
{
    "messageId": "tdei_dataset_id",
    "messageType": "workflow_identifier",
    "data": {
      "data_type":"osw|flex|pathways",
      "file_upload_path" : "file path",
      "success": true|false,
      "message": "message" // if false the error string else empty string
    } 
  }
```
