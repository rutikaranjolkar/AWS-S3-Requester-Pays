##!/usr/bin/env python

# Use this script to download an entire folder from the bucket

import boto3
import botocore
import os
from botocore.exceptions import ClientError

# CONFIGURATION
BUCKET_NAME = 'bucket-name-here'
S3 = boto3.client('s3')
paginator = S3.get_paginator('list_objects')

page_iterator = paginator.paginate(Bucket='bucket-name-here',RequestPayer='requester')

# END CONFIGURATION

def main():
    for page in page_iterator:
        print("Next Page : {} ".format(page['IsTruncated']))
        objects = page['Contents']
        for obj in objects:
            print(obj['Key'])
            try:
                if obj['Key'].endswith('/'):
                    if isAllowedFolder(obj['Key']):
                        os.makedirs(obj['Key'], exist_ok=True)
                       
                elif '/' in obj['Key']:
                    if isAllowedFile(obj['Key']):
                        download = S3.get_object(
                            Bucket=BUCKET_NAME,
                            Key=obj['Key'],
                            RequestPayer='requester'
                        )
 

                       # Object within directory
                        d = obj['Key']
                        os.makedirs(d[:d.rfind('/')], exist_ok=True)
                        with open(obj['Key'], 'wb') as f:
                            f.write(download['Body']._raw_stream.data)
                elif isAllowedFile(obj['Key']):
                    download = S3.get_object(
                            Bucket=BUCKET_NAME,
                            Key=obj['Key'],
                            RequestPayer='requester'
                        )
 
                    # Object at top level
                    with open(obj['Key'], 'wb') as f:
                        f.write(download['Body']._raw_stream.data)
                    print('OK!')
            except IOError as error:
                print('ERROR! Problem writing object to disk: {}'.format(error))
            except ClientError as error:
                print('ERROR! Problem getting object from S3: {}'.format(error))
            except KeyError as error:
                #sys.exit()
                print('ERROR! Key error {}'.format(error))

# User should modify the path as per their requirement
# In this example, the sample path is 'folder/subFolder'
# 
# The 'isAllowedFolder' method below takes path as an input
# Both 'folder/subFolder' and folder/subFolder/'are input
# Note the '/' at the end of the second value in quotes
# User should replace the values in quotes by their desired path 

def isAllowedFolder(path):
    return 'folder/subFolder' == path or 'folder/subFolder/' == path


# User should replace the value in quotes with their path as above

def isAllowedFile(path):
    return path.startswith('folder/subFolder')

if __name__ == '__main__':
    main()
