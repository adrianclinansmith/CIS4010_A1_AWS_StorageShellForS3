class S5:

    # Initializer

    def __init__(self, resource, root_bucket):
        self.resource = resource
        self.root_bucket = root_bucket
        self.current_bucket = root_bucket
        self.current_folder = '/'

    # Public methods

    def bucket_exists(self, bucket):
        self.resource.Bucket('priyajdm').creation_date is None

    # -must edit-
    def download(self, from_bucket, from_path):
        with open('test.txt', 'wb') as file:
            self._client().download_fileobj('cis4010-aclinans', 'README.txt', file)

    def upload(self, local_path, to_bucket, to_path):
        with open(local_path, 'rb') as local_file:
            self._client().upload_fileobj(local_file, to_bucket, to_path)

    def set_current_bucket(self, to_bucket):
        if self._bucket_exists(to_bucket) == False:
            raise Exception(f'No bucket named "{to_bucket}"')
        else:
            self.current_bucket = to_bucket

    def set_current_folder(self, to_folder):
        to_folder = to_folder.strip('/') + '/'
        if to_folder == '/' or self._current_bucket_has(to_folder):
            self.current_folder = to_folder
        else:
            ex = f'No folder "{to_folder}" in "{self.current_bucket}"'
            raise Exception(ex)



    # Private methods

    def _bucket_exists(self, bucket_name):
        bucket_object = self.resource.Bucket(bucket_name)
        if bucket_object.creation_date is None:
            return False
        return True

    def _client(self):
        return self.resource.meta.client

    def _current_bucket_object(self):
        return self.resource.Bucket(self.current_bucket)

    def _current_bucket_has(self, file_name):
        bucket_object = self._current_bucket_object()
        for file_in_bucket in bucket_object.objects.all():
            if (file_in_bucket.key == file_name):
                return True
        return False
