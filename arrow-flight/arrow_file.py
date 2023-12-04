import pyarrow.csv as csv
import pyarrow.ipc as ipc
import pyarrow as pa

def read_csv_to_arrow(csv_file_path):
    """Reads a CSV file and converts it to an Arrow Table."""
    arrow_table = csv.read_csv(csv_file_path)
    return arrow_table

def write_arrow_to_file(arrow_table, arrow_file_path):
    """Writes an Arrow Table to a .arrow file."""
    with open(arrow_file_path, 'wb') as sink:
        writer = ipc.new_file(sink, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()

def read_arrow_from_file(arrow_file_path):
    """Reads an Arrow Table from a .arrow file."""
    with open(arrow_file_path, 'rb') as source:
        reader = ipc.open_file(source)
        arrow_table = reader.read_all()
    return arrow_table

def arrow_to_csv(arrow_table, csv_file_path):
    """Writes an Arrow Table to a CSV file."""
    csv.write_csv(arrow_table, csv_file_path)

# Example usage
csv_file_path = 'SampleData.csv'  # Replace with your CSV file path
arrow_file_path = 'output.arrow'  # Replace with your desired .arrow file path
output_csv_file_path = 'SampleOutput.csv'  # Replace with your desired output CSV file path

# Convert CSV to Arrow Table
arrow_table = read_csv_to_arrow(csv_file_path)

# Write Arrow Table to .arrow file
write_arrow_to_file(arrow_table, arrow_file_path)

# Read Arrow Table from .arrow file
read_arrow_table = read_arrow_from_file(arrow_file_path)

# Convert Arrow Table to CSV
arrow_to_csv(read_arrow_table, output_csv_file_path)

# Print the table to verify
print("Arrow Table read from .arrow file:")
print(read_arrow_table)
