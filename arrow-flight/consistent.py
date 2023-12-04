import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, num_vnodes =100, num_replicas = 3):
        self.num_vnodes = num_vnodes
        self.num_replicas = num_replicas
        self.ring = []
        self.node_data = {}
        self.data_keys = {}
    def hash(self , key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
    def add_node(self , node):
        if node not in self.node_data:
            self.node_data[node] = {}
        for i in range(self.num_vnodes):
            