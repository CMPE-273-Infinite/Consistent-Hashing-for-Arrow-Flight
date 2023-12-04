# flight_server.py
import pyarrow.flight as flight
import pyarrow as pa
import io
from consistent_hashing_aws import S3ConsistentHashing  # Ensure this class is defined correctly

class S3FlightServer(flight.FlightServerBase):
    def __init__(self, location, s3_buckets, **kwargs):
        super().__init__(location, **kwargs)
        self.hashing = S3ConsistentHashing(s3_buckets)

    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all()
        df = table.to_pandas()

        for index, row in df.iterrows():
            file_path = row['file_path']
            file_key = row['file_key']
            self.hashing.store_file(file_path, file_key)
            print(f"Stored file: {file_key} from path: {file_path}")

    def do_get(self, context, ticket):
        command, file_key = ticket.ticket.decode().split(',')

        if command == 'delete_file':
            self.hashing.delete_file(file_key)
            print(f"Deleted file: {file_key}")
            return flight.RecordBatchStream(pa.RecordBatchStreamReader(io.BytesIO(b'')))
        else:
            raise flight.FlightUnimplementedError('Unknown command')

def main():
    location = "grpc://0.0.0.0:8815"
    s3_buckets = ['bucket1', 'bucket2', 'bucket3']  # Replace with your actual bucket names
    server = S3FlightServer(location, s3_buckets)
    print(f"Starting server at {location}")
    server.serve()

if __name__ == '__main__':
    main()
