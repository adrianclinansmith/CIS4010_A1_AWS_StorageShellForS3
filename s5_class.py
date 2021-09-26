import os
from s5_exception import S5Exception

class S5:

    # Initializer

    def __init__(self, resource):
        self.resource = resource
        self.current_bucket = ''
        self.current_folder = '/'

    # Public methods

    def cloud_to_local_copy(self, from_s3_path, to_local_file):
        bucket_name, s3_file_name = self._resolve_path_name(from_s3_path)
        s3_file_name = s3_file_name.lstrip('/')
        if not self._bucket_has_file(bucket_name, s3_file_name):
            e = f'No file/permission for "{s3_file_name}" in "{bucket_name}"'
            raise S5Exception(e)
        with open(to_local_file, 'wb') as fileobj:
            self._client().download_fileobj(bucket_name, s3_file_name, fileobj)

    def local_to_cloud_copy(self, local_file, to_s3_path):
        bucket_name, s3_file_name = self._resolve_path_name(to_s3_path)
        s3_file_name = s3_file_name.lstrip('/')
        if self._bucket_has_file(bucket_name, s3_file_name):
            e = f'"{s3_file_name}" already exists in "{bucket_name}"'
            raise S5Exception(e)
        with open(local_file, 'rb') as fileobj:
            self._client().upload_fileobj(fileobj, bucket_name, s3_file_name)

    def set_current_path(self, path_name):
        if not path_name or path_name == '/':
            self.current_bucket = ''
            self.current_folder = '/' 
            return
        bucket_name, file_name = self._resolve_path_name(path_name)
        folder_name = file_name.rstrip('/') + '/'
        if self._bucket_has_file(bucket_name, folder_name.lstrip('/')):
            self.current_bucket = bucket_name
            self.current_folder = folder_name
        else:
            e = f'No folder/permission for "{folder_name}" in "{bucket_name}"'
            raise S5Exception(e)

    # Private methods

    def _bucket_exists(self, bucket_name):
        bucket_object = self.resource.Bucket(bucket_name)
        if bucket_object.creation_date is None:
            return False
        return True

    def _bucket_has_file(self, bucket_name, file_name):
        if not self._bucket_exists(bucket_name):
            raise S5Exception(f'No bucket named "{bucket_name}"')
        elif not file_name or file_name == '/':
            return True
        bucket_as_object = self.resource.Bucket(bucket_name)
        for file_in_bucket in bucket_as_object.objects.all():
            if (file_in_bucket.key == file_name):
                return True
        return False

    def _client(self):
        return self.resource.meta.client

    def _resolve_path_name(self, path_name):
        bucket_name, file_name = '', ''
        if ':' in path_name:
            bucket_name = path_name.split(':')[0]
            file_name = '/' + path_name.split(':')[1].lstrip('/')
        else:
            if not self.current_bucket:
                bucket_name = path_name
                file_name = '/'
            elif path_name.startswith('/'):
                bucket_name = self.current_bucket
                file_name = path_name
            else:
                bucket_name = self.current_bucket
                file_name = self.current_folder + path_name
        return (bucket_name, os.path.normpath(file_name))
        

