##!/usr/bin/env python

# Use this script to download entir bucket

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
                    os.makedirs(obj['Key'], exist_ok=True)
 
                elif '/' in obj['Key']:
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
            except IOError as error:
                print('ERROR! Problem writing object to disk: {}'.format(error))
            except ClientError as error:
                print('ERROR! Problem getting object from S3: {}'.format(error))
            except KeyError as error:
                #sys.exit()
                print('ERROR! Key error {}'.format(error))


if __name__ == '__main__':
    main()
