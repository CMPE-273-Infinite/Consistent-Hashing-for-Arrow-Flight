import hashlib

class ConsistentHashRing:
    def __init__(self, buckets):
        self.ring = dict()
        self.sorted_keys = []
        for bucket in buckets:
            self.add_bucket(bucket)

    def add_bucket(self, bucket):
        key = self._hash_key(bucket)
        self.ring[key] = bucket
        self.sorted_keys.append(key)
        self.sorted_keys.sort()

        self._redistribute_data_for_new_bucket(key)

    def remove_bucket(self, bucket):
        key = self._hash_key(bucket)
        next_bucket = self._get_next_bucket(key)
        self._redistribute_data(bucket, next_bucket)
        del self.ring[key]
        self.sorted_keys.remove(key)

    def get_bucket_for_file(self, filename):
        key = self._hash_key(filename)
        for i in self.sorted_keys:
            if key <= i:
                return self.ring[i]
        return self.ring[self.sorted_keys[0]]

    def _get_next_bucket(self, key):
        for i in self.sorted_keys:
            if key < i:
                return self.ring[i]
        return self.ring[self.sorted_keys[0]]

    def _redistribute_data(self, from_bucket, to_bucket):
        # Placeholder for data redistribution logic
        print(f"Redistributing data from {from_bucket} to {to_bucket}")

    def _redistribute_data_for_new_bucket(self, new_bucket_key):
        next_bucket_key = self._get_next_bucket_key(new_bucket_key)
        from_bucket = self.ring[next_bucket_key]
        to_bucket = self.ring[new_bucket_key]
        print(f"Redistributing data from {from_bucket} to {to_bucket} for range {new_bucket_key} to {next_bucket_key}")

    def _get_next_bucket_key(self, key):
        for i in self.sorted_keys:
            if key < i:
                return i
        return self.sorted_keys[0]

    @staticmethod
    def _hash_key(key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % 360


# Example Usage
buckets = ['s3_bucket1', 's3_bucket3']
ring = ConsistentHashRing(buckets)

# Add a new bucket
ring.add_bucket('s3_bucket2')
