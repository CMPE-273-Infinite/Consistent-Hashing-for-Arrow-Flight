import boto3
import hashlib
import bisect

class S3ConsistentHashing:
    def __init__(self, s3_bucket_names, num_vnodes=100, num_replicas=3):
        self.s3_client = boto3.client('s3')
        self.num_vnodes = num_vnodes
        self.num_replicas = num_replicas
        self.ring = []
        self.buckets = s3_bucket_names
        for bucket in s3_bucket_names:
            self.add_node(bucket)

    def _hash(self, key):
        """Return a hash value for a given key."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """Add a node (S3 bucket) to the hash ring."""
        for i in range(self.num_vnodes):
            vnode_key = f'{node}-{i}'
            hash_value = self._hash(vnode_key)
            bisect.insort(self.ring, (hash_value, node))

    def _get_replica_nodes(self, key):
        """Get a list of nodes (S3 buckets) where the replicas for the key should be stored."""
        hash_value = self._hash(key)
        nodes = []
        for hash_val, node in self.ring:
            if hash_val >= hash_value and node not in nodes:
                nodes.append(node)
                if len(nodes) == self.num_replicas:
                    break
        return nodes + [node for _, node in self.ring[:self.num_replicas - len(nodes)]]

    def store_file(self, file_path, file_key):
        """Store a file in S3 buckets determined by consistent hashing."""
        nodes = self._get_replica_nodes(file_key)
        for node in nodes:
            self.s3_client.upload_file(file_path, node, file_key)
        print(f"File '{file_key}' replicated on S3 buckets: {nodes}")

    def delete_file(self, file_key):
        """Delete a file from all S3 buckets where its replicas are stored."""
        nodes = self._get_replica_nodes(file_key)
        for node in nodes:
            try:
                self.s3_client.delete_object(Bucket=node, Key=file_key)
                print(f"File '{file_key}' deleted from S3 bucket '{node}'")
            except self.s3_client.exceptions.NoSuchKey:
                print(f"File '{file_key}' not found in S3 bucket '{node}'")
            except Exception as e:
                print(f"Error deleting file '{file_key}' from S3 bucket '{node}': {e}")

# Example Usage
s3_bucket_names = ['bucket1', 'bucket2', 'bucket3']  # Replace with your actual bucket names
ch = S3ConsistentHashing(s3_bucket_names)

# Store a file in S3 using consistent hashing
ch.store_file('path/to/your/file.txt', 'file_key')

# Delete a file from S3 using consistent hashing
ch.delete_file('file_key')

