from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "centered-sol-469812-v8"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
    "jobName": "bq-load",  # Provide a unique name for the job
    "parameters": {
        "javascriptTextTransformGcsPath": "gs://gs://my-bucket-88/udf.js",
        "JSONPath": "gs://my-bucket-88/bq_schema.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "centered-sol-469812-v8:sales_data.sales",
        "inputFilePattern": "gs://my-bucket-88/sales.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://my-bucket-88"
    }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)
