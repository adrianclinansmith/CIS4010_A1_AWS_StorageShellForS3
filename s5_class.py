class S5:

    # Initializer

    def __init__(self, resource, root_bucket):
        self.resource = resource
        self.root_bucket = root_bucket
        self.current_bucket = root_bucket
        self.current_folder = '/'

    # Public methods

    # -must edit-
    def download(self, from_bucket, from_path):
        with open('test.txt', 'wb') as file:
            self._client().download_fileobj('cis4010-aclinans', 'README.txt', file)

    def upload(self, local_path, to_bucket, to_path):
        with open(local_path, 'rb') as local_file:
            self._client().upload_fileobj(local_file, to_bucket, to_path)

    def resolve_path(self, path):
        bucket_name, file_name = self._get_bucket_and_file_name(path)
        destination_bucket = self.current_bucket
        if bucket_name:
            destination_bucket = bucket_name
        destination_file = '/'
        if file_name.startswith('/'):
            destination_file = file_name
        elif bucket_name:
            destination_file = '/' + file_name
        elif file_name:
            destination_file = self.current_folder + file_name
        return (destination_bucket, destination_file)

    def set_current_folder(self, to_bucket, to_folder):
        ex_string = f'No bucket named "{to_bucket}"'
        assert self._bucket_exists(to_bucket), ex_string
        to_folder = to_folder.strip('/') + '/'
        if to_folder == '/':
            self.current_folder = '/'
        elif self._bucket_has_file(to_bucket, to_folder):
            self.current_folder = '/' + to_folder
        else:
            ex = f'No file or permission for "{to_folder}" in "{to_bucket}"'
            raise Exception(ex)
        self._set_current_bucket(to_bucket)

    # Private methods

    def _bucket_exists(self, bucket_name):
        bucket_object = self.resource.Bucket(bucket_name)
        if bucket_object.creation_date is None:
            return False
        return True

    def _bucket_has_file(self, bucket_name, file_name):
        if not self._bucket_exists(bucket_name):
            raise Exception(f'No bucket named "{bucket_name}"')
        bucket_as_object = self.resource.Bucket(bucket_name)
        for file_in_bucket in bucket_as_object.objects.all():
            if (file_in_bucket.key == file_name):
                return True
        return False

    def _client(self):
        return self.resource.meta.client

    def _get_bucket_and_file_name(self, from_path):
        bucket, file_path = '', ''
        if ':' in from_path:
            bucket = from_path.split(':', 1)[0]
            file_path = from_path.split(':', 1)[1]
        else:
            file_path = from_path
        return (bucket, file_path)

    def _set_current_bucket(self, to_bucket):
        if not self._bucket_exists(to_bucket):
            raise Exception(f'No bucket named "{to_bucket}"')
        else:
            self.current_bucket = to_bucket
        

