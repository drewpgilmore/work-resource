# GENERATES AWS SESSION
# Returns access keys to local config file 

import pandas as pd 
import datetime as dt
import numpy as np
import os
from configparser import ConfigParser
import boto3

# --------------------------------- 
# AWS CONNECTION FOR FILE DOWNLOADS
# 1. Call alohomora.exe from bash window 
# 2. Enter AWS credentials
# 3. Credentials will be saved in H:/.aws/credentials or C:/Users/{name}/.aws/credentials


netcracker_bucket = 'v******-sms-prodbdcs-us-west-2-in'
archive_bucket    = 'v******-sms-prodbdcs-us-west-2-archive'

from config import aws_credentials
# FROM config file
# password = '******'
# aws_credentials = 'C:/Users/dgilmore/.aws/credentials'
# user = '******'

try:
    filename = aws_credentials
except:
    filename = 'H:/.aws/credentials'

config = ConfigParser()
config.read(filename)

from config import user

access_key        = config.get(user, 'aws_access_key_id'    ) 
secret_access_key = config.get(user, 'aws_secret_access_key') 
session_token     = config.get(user, 'aws_session_token'    ) 

s3 = boto3.client(
    's3', 
    aws_access_key_id     = access_key, 
    aws_secret_access_key = secret_access_key, 
    aws_session_token     = session_token
)

# AWS Session initiated
# ---------------------------------