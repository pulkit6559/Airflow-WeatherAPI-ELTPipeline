from aiohttp import ClientError
import boto3

# Let's use Amazon S3
s3 = boto3.resource('s3')

# Replace YOUR_BUCKET_NAME with the name of your bucket
bucket_name = 'iambucketnew'

try:
    # Create the bucket
    s3.create_bucket(Bucket=bucket_name,
                     CreateBucketConfiguration={
                        'LocationConstraint': 'eu-central-1'
                    })
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print(f"Successfully created bucket {bucket_name}")
    