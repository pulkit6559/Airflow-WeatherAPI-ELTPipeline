import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def s3_single_upload(local_file_path, bucket_name, object_name):
    s3 = boto3.resource('s3')
    
    try:
        s3.Object(bucket_name, object_name).upload_file(local_file_path)
        return True
    except Exception as e:
        print(e)
        return False

def s3_multipart_upload(file_path, bucket_name, object_key, region_name=None):
    """
    Performs an S3 multipart upload for a specified local file.

    Parameters:
    - file_path: Local file path to upload
    - bucket_name: S3 bucket name
    - object_key: Key to assign to the S3 object
    - region_name (optional): AWS region name

    Returns:
    - None if the upload is successful, raises an exception otherwise.
    """

    # Create an S3 client
    s3 = boto3.client('s3', region_name=region_name)

    # Set the multipart threshold to 8 MB (minimum allowed by S3)
    multipart_threshold = 8 * 1024 * 1024

    # Set the multipart chunk size to 8 MB (minimum allowed by S3)
    multipart_chunksize = 8 * 1024 * 1024

    # Create a multipart upload request
    response = s3.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key
    )

    upload_id = response['UploadId']

    try:
        # Perform the multipart upload
        with open(file_path, 'rb') as file:
            part_number = 1
            parts = []

            while True:
                # Read a chunk of the file
                chunk = file.read(multipart_chunksize)

                # Break if we reached the end of the file
                if not chunk:
                    break

                # Upload the chunk as a part
                response = s3.upload_part(
                    Bucket=bucket_name,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk
                )

                # Append the part information to the list
                parts.append({'PartNumber': part_number, 'ETag': response['ETag']})

                part_number += 1

        # Complete the multipart upload
        response = s3.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        print("Multipart upload successful.")
        print(response)

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        raise e
    except Exception as e:
        print(f"Error during multipart upload: {e}")
        raise e
    finally:
        # Clean up in case of an exception
        s3.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)

# # usage:
# file_path = '/path/to/your/file.txt'
# bucket_name = 'your-s3-bucket'
# object_key = 'your-object-key'
# aws_access_key_id = 'your-access-key-id'
# aws_secret_access_key = 'your-secret-access-key'
# region_name = 'your-region'

# s3_multipart_upload(file_path, bucket_name, object_key, aws_access_key_id, aws_secret_access_key, region_name)
