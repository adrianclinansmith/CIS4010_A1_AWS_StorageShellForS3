Requirements and Running
===============================================================================

Must use Python 3.5 or higher. In the terminal enter:
    $ python s5.py
or possibly:
    $ python3 s5.py


AWS authentication
===============================================================================

S5-S3conf must be in the same directory as the python script, and it must
be the same as the credentials file specified at: 
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html

In particular, it must have two lines like so:

aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY