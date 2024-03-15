# TDEI-extract-load-service
TDEI extract load service to unzip the dataset files and load the records to the database.

## Message Extract Load Request 

```json
{
    "messageId": "tdei_dataset_id|job_id",
    "messageType": "workflow_identifier",
    "data": {
      "data_type":"osw|flex|pathways",
      "file_upload_path" : "file path",
      "tdei_dataset_id" : "tdei_dataset_id"
    } 
  }
```

## Message Extract Load Response 

```json
{
    "messageId": "tdei_dataset_id|job_id",
    "messageType": "workflow_identifier",
    "data": {
      "success": true|false,
      "message": "message" // if false the error string else empty string
    } 
  }
```

An example env file can be found at `.env-example`