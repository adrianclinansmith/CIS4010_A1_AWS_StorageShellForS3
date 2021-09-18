import boto3
import botocore
import subprocess
import sys
import s5_class

###############################################################################
# Function Definitions
###############################################################################

def attempt_command(s5, string):
    tokens = string.split()
    if not tokens:
        return False 
    elif tokens[0] == 'ccopy':
        ccopy(tokens)
    elif tokens[0] == 'cdelete':
        cdelete(tokens)
    elif tokens[0] == 'ch_folder':
        ch_folder(s5, tokens)
    elif tokens[0] == 'cl_copy':
        cl_copy(tokens)
    elif tokens[0] == 'create_bucket':
        create_bucket(tokens)
    elif tokens[0] == 'create_folder':
        create_folder(tokens)
    elif tokens[0] == 'cwf':
        cwf(s5)
    elif tokens[0] == 'delete_bucket':
        delete_bucket(tokens)
    elif tokens[0] == 'exit':
        exit()
    elif tokens[0] == 'lc_copy':
        lc_copy(s5, tokens)
    elif tokens[0] == 'list':
        s3_list(tokens)
    else:
        return False
    return True

def ccopy(command_tokens):
    print('execute ccopy')

def cdelete(command_tokens):
    print('execute cdelete')

def ch_folder(s5, command_tokens):
    try:
       bucket, file_path = resolve_path(command_tokens[1])
    except IndexError:
        bucket, file_path = '', ''
    try:
        if bucket:
            s5.set_current_bucket(bucket)
        elif not file_path or file_path.startswith('/'):
            s5.set_current_bucket(s5.root_bucket)
        s5.set_current_folder(file_path)
    except Exception as ex:
        print(ex, file=sys.stderr)

    
def cl_copy(command_tokens):
    print('execute cl_copy')

def create_bucket(command_tokens):
    print('execute create_bucket')

def create_folder(command_tokens):
    print('execute create_folder')

def cwf(s5):
    print(s5.current_bucket + ':' + s5.current_folder)

def delete_bucket(command_tokens):
    print('execute delete_bucket')

def exit():
    print('execute exit')
    sys.exit(0)

def lc_copy(s5, command_tokens):
    local_path, to_bucket, to_path = '', '', ''
    try:
        local_path = command_tokens[1]
        to_bucket, to_path = resolve_path(command_tokens[2])
        assert to_bucket is not None
    except:
        print('usage: lc_copy local_path bucket:s3_full_path', file=sys.stderr)
        return
    try:
        s5.upload(local_path, to_bucket, to_path)
    except FileNotFoundError:
        print(f'"{local_path}" could not be opened', file=sys.stderr)
    except:
        print('Unsuccessful copy', file=sys.stderr)   

def s3_list(command_tokens):
    print('execute list')

# helper functions

def resolve_path(path):
    bucket, file_path = '', ''
    if ':' in path:
        bucket = path.split(':', 1)[0]
        file_path = path.split(':', 1)[1]
    else:
        bucket = None
        file_path = path
    return (bucket, file_path)
    
    

###############################################################################
# Main
###############################################################################

print('Welcome to the AWS S3 Storage Shell (S5)')

# read access keys from S5-S3conf
access_keys = []
try:
    with open('S5-S3conf', 'r') as file:
        file_lines = file.readlines()
        access_keys.append(file_lines[0].strip())
        access_keys.append(file_lines[1].strip())
except (FileNotFoundError, IndexError):
    print('You could not be connected to your S3 storage')
    print('Please review procedures for authenticating your account on AWS S3')
    sys.exit(1)

# connect to the s3 client
try:
    s3_resource = boto3.resource('s3',
        aws_access_key_id = access_keys[0],
        aws_secret_access_key = access_keys[1])
    s3_resource.meta.client.list_buckets()
except:
    print('You could not be connected to your S3 storage')
    print('Please review procedures for authenticating your account on AWS S3')
    sys.exit(1)


s5 = s5_class.S5(s3_resource, 'cis4010-aclinans')

print('You are now connected to your S3 storage')

# shell loop
while True:
    print('s5> ', end='')
    user_string = input()
    did_execute = attempt_command(s5, user_string)
    if did_execute == False:
        subprocess.run(user_string, shell=True)