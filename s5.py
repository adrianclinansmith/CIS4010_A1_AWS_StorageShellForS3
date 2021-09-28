import boto3
import botocore
import subprocess
import sys
import s5_class
from s5_exception import S5Exception
import os

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
        create_bucket(s5, tokens)
    elif tokens[0] == 'create_folder':
        create_folder(s5, tokens)
    elif tokens[0] == 'cwf':
        cwf(s5)
    elif tokens[0] == 'delete_bucket':
        delete_bucket(tokens)
    elif tokens[0] == 'exit':
        exit()
    elif tokens[0] == 'lc_copy':
        lc_copy(s5, tokens)
    elif tokens[0] == 'list':
        s3_list(s5, tokens)
    else:
        return False
    return True

def ccopy(command_tokens):
    print('execute ccopy')

def cdelete(command_tokens):
    print('execute cdelete')

def ch_folder(s5, command_tokens):
    first_arg = ''
    if len(command_tokens) > 1:
        first_arg = command_tokens[1]
    try:
        s5.set_current_path(first_arg)
    except S5Exception as ex:
        print('s5 ex:', ex, file=sys.stderr)
    except Exception as ex:
        print('ex:', ex, file=sys.stderr)
  
def cl_copy(command_tokens):
    try:
        s5.cloud_to_local_copy(command_tokens[1], command_tokens[2])
    except IndexError:
        print('usage: cl_copy bucket:s3_path local_path', file=sys.stderr)
    except FileNotFoundError:
        print(f'"{command_tokens[2]}" could not be opened', file=sys.stderr)
    except S5Exception as ex:
        print(ex, file=sys.stderr)  
    except:
         print('Unsuccessful copy', file=sys.stderr)   

def create_bucket(s5, command_tokens):
    try:
        s5.create_bucket(command_tokens[1])
    except IndexError:
        print('usage: create_bucket bucket_name', file=sys.stderr)
    except Exception as ex:
        print("create_bucket error: ", ex, file=sys.stderr) 

def create_folder(s5, command_tokens):
    try:
        s5.create_folder(command_tokens[1])
    except IndexError:
        print('usage: create_folder folder_name', file=sys.stderr)
    except Exception as ex:
        print("create_folder error: ", ex, file=sys.stderr) 

def cwf(s5):
    if not s5.current_bucket:
        print ('/')
    else:
        print(s5.current_bucket + ':' + s5.current_folder)

def delete_bucket(command_tokens):
    print('execute delete_bucket')

def exit():
    print('Goodbye')
    sys.exit(0)

def lc_copy(s5, command_tokens):
    try:
        s5.local_to_cloud_copy(command_tokens[1], command_tokens[2])
    except IndexError:
        print('usage: lc_copy local_path bucket:s3_path', file=sys.stderr)
    except FileNotFoundError:
        print(f'"{command_tokens[1]}" could not be opened', file=sys.stderr)
    except S5Exception as ex:
        print(ex, file=sys.stderr)  
    except:
         print('Unsuccessful copy', file=sys.stderr) 

def s3_list(s5, command_tokens):
    try:
        s5.list_current_contents()
    except Exception as ex:
         print('Couldn\'t list: ', ex, file=sys.stderr) 
    
    

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


s5 = s5_class.S5(s3_resource)

print('You are now connected to your S3 storage')

# shell loop
while True:
    print('s5> ', end='')
    user_string = input()
    did_execute = attempt_command(s5, user_string)
    if did_execute == False:
        subprocess.run(user_string, shell=True)