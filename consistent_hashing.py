import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, num_vnodes=100, num_replicas=3):
        self.num_vnodes = num_vnodes
        self.num_replicas = num_replicas
        self.ring = []
        self.node_data = {}
        self.data_keys = {}

    def _hash(self, key):
        """Return a hash value for a given key."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """Add a node, represented by a string 'node'."""
        if node not in self.node_data:
            self.node_data[node] = {}
        for i in range(self.num_vnodes):
            vnode_key = f'{node}-{i}'
            hash_value = self._hash(vnode_key)
            bisect.insort(self.ring, (hash_value, node))

    def remove_node(self, node):
        """Remove a node and migrate its data."""
        if node in self.node_data:
            # Migrate data before removing the node
            for key in list(self.node_data[node].keys()):
                self._migrate_data(key, node)
            del self.node_data[node]

        self.ring = [(hash_value, n) for hash_value, n in self.ring if n != node]

    def _migrate_data(self, key, old_node=None):
        """Migrate or replicate data to appropriate nodes."""
        nodes = self._get_replica_nodes(key)
        if old_node and old_node in nodes:
            nodes.remove(old_node)
        value = self.data_keys.get(key)
        if value is not None:
            for node in nodes:
                self.node_data[node][key] = value

    def _get_replica_nodes(self, key):
        """Get a list of nodes where the replicas for the key should be stored."""
        hash_value = self._hash(key)
        nodes = []
        for hash_val, node in self.ring:
            if hash_val >= hash_value and node not in nodes:
                nodes.append(node)
                if len(nodes) == self.num_replicas:
                    break
        return nodes + [node for _, node in self.ring[:self.num_replicas - len(nodes)]]

    def set_data(self, key, value):
        """Assign a value to a key and replicate it."""
        self.data_keys[key] = value
        nodes = self._get_replica_nodes(key)
        for node in nodes:
            self.node_data[node][key] = value
        print(f"Data '{key}' replicated on nodes: {nodes}")

    def get_data(self, key):
        """Retrieve a value for a key from one of the replica nodes."""
        nodes = self._get_replica_nodes(key)
        for node in nodes:
            if key in self.node_data[node]:
                return self.node_data[node][key]
        return None

# # Example Usage
# ch = ConsistentHashing()
# ch.add_node('node1')
# ch.add_node('node2')
# ch.add_node('node3')

# # Set data with replication
# ch.set_data('my_key', '123')

# # Remove a node and check data availability
# ch.remove_node('node2')
# value = ch.get_data('my_key')
# print(f'After removing node2, the value for my_key is still available: {value}')
