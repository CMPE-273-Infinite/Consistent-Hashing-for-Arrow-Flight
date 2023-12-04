# flight_client.py
import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd

class S3FlightClient:
    def __init__(self, location):
        self.client = flight.FlightClient(location)

    def upload_file(self, file_path, file_key):
        df = pd.DataFrame({'file_path': [file_path], 'file_key': [file_key]})
        table = pa.Table.from_pandas(df)

        descriptor = flight.FlightDescriptor.for_command('store_file')
        writer, _ = self.client.do_put(descriptor, table.schema)
        writer.write_table(table)
        writer.close()

    def delete_file(self, file_key):
        descriptor = flight.FlightDescriptor.for_command('delete_file')
        ticket = flight.Ticket(f'delete_file,{file_key}'.encode())

        reader = self.client.do_get(ticket)
        reader.read_all()

def main():
    location = "grpc://localhost:8815"
    client = S3FlightClient(location)

    client.upload_file('path/to/your/file.txt', 'file_key')
    client.delete_file('file_key')

if __name__ == '__main__':
    main()
