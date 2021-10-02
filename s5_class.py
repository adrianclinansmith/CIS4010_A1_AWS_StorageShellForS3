from collections import namedtuple
import os
from s5_exception import S5Exception

class S5:

    ###########################################################################
    # Initializer
    ###########################################################################

    def __init__(self, resource):
        self.resource = resource
        self.current_bucket = ''
        self.current_folder = '/'

    ###########################################################################
    # Public methods
    ###########################################################################

    def cloud_to_cloud_copy(self, from_s3_file, to_s3_file):
        source_path = self._resolve_s3_path(from_s3_file, from_root=True)
        new_path = self._resolve_s3_path(to_s3_file)
        source = source_path.bucket + source_path.file
        s3_object = self.resource.Object(new_path.bucket, new_path.file)
        s3_object.copy_from(CopySource=source)

    def cloud_to_local_copy(self, from_s3_file, to_local_file):
        path = self._resolve_s3_path(from_s3_file)
        if not self._bucket_has_file(path.bucket, path.file):
            e = f'No file/permission for "{path.file}" in "{path.bucket}"'
            raise S5Exception(e)
        with open(to_local_file, 'wb') as fileobj:
            self._client().download_fileobj(path.bucket, path.file, fileobj)

    def create_bucket(self, bucket_name):
        self._client().create_bucket(Bucket=bucket_name)

    def create_folder(self, s3_path):
        path = self._resolve_s3_path(s3_path, as_folder=True)
        self._client().put_object(Bucket=path.bucket, Key=path.file)

    def delete_bucket(self, bucket_name):
        bucket_object = self._bucket_as_object(bucket_name)
        if not bucket_object:
            raise S5Exception(f'No bucket named "{bucket_name}"')
        bucket_object.delete()

    def delete_file_at(self, s3_path):
        file_object = self._object_from_path(s3_path)
        if not file_object:
            raise S5Exception(f'No file/permission for "{s3_path}"')
        elif file_object.key.endswith('/') and self._contents_of(file_object):
            raise S5Exception(f'Cannot delete non-empty folder "{s3_path}"')
        else:
            file_object.delete()

    def print_contents(self, s3_path, *, show_details=False):
        path = self._path_tuple(self.current_bucket, self.current_folder)
        if s3_path:
            path = self._resolve_s3_path(s3_path, as_folder=True)
        if not path.bucket:
            self._print_buckets(show_details=show_details)
            return
        file_object = self._object_from_path(path.bucket + ':' + path.file)
        if file_object and not file_object.key.endswith('/'):
            print(file_object.key)
            return
        folder = path.file.lstrip('/')
        for object in self._folder_contents(path.bucket, path.file):
            object_name = object.key[len(folder):]
            if show_details:
                print(object_name, object.content_type)
            else:
                print(object_name)

    def local_to_cloud_copy(self, from_local_file, to_s3_file):
        path = self._resolve_s3_path(to_s3_file)
        if self._bucket_has_file(path.bucket, path.file):
            e = f'"{path.file}" already exists in "{path.bucket}"'
            raise S5Exception(e)
        with open(from_local_file, 'rb') as fileobj:
            self._client().upload_fileobj(fileobj, path.bucket, path.file)

    def set_current_path(self, path_name):
        if not path_name or path_name == '/':
            self.current_bucket = ''
            self.current_folder = '/' 
            return
        path = self._resolve_s3_path(path_name, from_root=True, as_folder=True)
        if self._bucket_has_file(path.bucket, path.file.lstrip('/')):
            self.current_bucket = path.bucket
            self.current_folder = path.file
        else:
            e = f'No folder/permission for "{path.file}" in "{path.bucket}"'
            raise S5Exception(e)

    ###########################################################################
    # Private methods
    ###########################################################################

    def _bucket_as_object(self, bucket_name):
        bucket_object = self.resource.Bucket(bucket_name)
        if bucket_object.creation_date is None:
            return None
        return bucket_object

    def _bucket_contents(self, bucket_name):
        bucket_resource = self.resource.Bucket(bucket_name)
        return bucket_resource.objects.all()

    def _bucket_has_file(self, bucket_name, file_name):
        bucket_object = self._bucket_as_object(bucket_name)
        if not bucket_object:
            raise S5Exception(f'No bucket named "{bucket_name}"')
        elif not file_name or file_name == '/':
            return True
        for file_object in bucket_object.objects.all():
            if (file_object.key == file_name):
                return True
        return False

    def _client(self):
        return self.resource.meta.client

    def _object_from_path(self, s3_path):
        path = self._resolve_s3_path(s3_path)
        try:
            file_object = self.resource.Object(path.bucket, path.file)
            file_object.content_type
            return file_object
        except:
            folder_name = path.file.rstrip('/') + '/'
        try:
            folder_object = self.resource.Object(path.bucket, folder_name)
            folder_object.content_type
            return folder_object
        except:
            return None

    def _contents_of(self, folder_object):
        key = folder_object.key
        if not key.endswith('/'):
            return [folder_object]
        all_objects = self.resource.Bucket(folder_object.bucket_name).objects.all()
        summaries = [x for x in all_objects if x.key.startswith(key) and x.key != key]
        return list(map(lambda x: x.Object(), summaries))
 
    def _folder_contents(self, bucket_name, folder_name):
        contents = []
        folder_name = folder_name.lstrip('/')
        for object_summary in self._bucket_contents(bucket_name):
            key = object_summary.key 
            if not key.startswith(folder_name) or key == folder_name:
                continue
            name = key[len(folder_name):]
            if ('/' not in name) or (name.find('/') == len(name) - 1):
                contents.append(object_summary.Object())
        return contents

    def _path_tuple(self, bucket_name, file_name):
        return namedtuple('Path', 'bucket file')(bucket_name, file_name)

    def _print_buckets(self, *, show_details=False):
        for bucket_data in self._client().list_buckets()['Buckets']:
            if show_details:
                name = bucket_data['Name']
                creation_date = bucket_data['CreationDate']
                print('{0:<30} {1}'.format(name, creation_date))
            else:
                print(bucket_data['Name'])  

    def _resolve_s3_path(self, path_name, *,from_root=False, as_folder=False):
        bucket_name, file_name = '', ''
        if path_name == '/':
            bucket_name = ''
            file_name = '/'
        elif ':' in path_name:
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
        file_name = os.path.normpath(file_name).replace('\\', '/')
        if as_folder:
            file_name = file_name.rstrip('/') + '/'
        if not from_root:
            file_name = file_name.lstrip('/')
        return self._path_tuple(bucket_name, file_name)
        

