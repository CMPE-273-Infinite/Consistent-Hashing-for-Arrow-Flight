from consistent import ConsistentHashing
from flight_server import NumberFlightServer
import pyarrow.flight as flight
if __name__ == '__main__':
    # ch_ring = ConsistentHashing(nodes=['A', 'B', 'C', 'D', 'E'], replica_count=2)
    # ch_ring.add_node('F')  # Add more nodes as needed

    location = flight.Location.for_grpc_tcp("localhost", 50051)
    server = NumberFlightServer(location)
    print("Starting the server on localhost:50051")
    server.serve()
