from fastapi import FastAPI, File, UploadFile
from pyarrow import csv as pa_csv
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import pyarrow as pa

import io
from s3_handler import S3Handler
app = FastAPI()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    # Read the CSV file into an Arrow Table
    content = await file.read()
    arrow_table = pa_csv.read_csv(io.BytesIO(content))
    print(arrow_table)
    arrow_file_path = file.filename
    with open(arrow_file_path, 'wb') as sink:
        writer = ipc.new_file(sink, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()
    S3Handler.upload_file(arrow_file_path,'box-office-team-infinite-loop', arrow_file_path)
    # Process the Arrow Table as needed
    # For example, just print the table for demonstration purposes
    print("here")

    return {"filename": file.filename, "message": "File processed successfully"}

