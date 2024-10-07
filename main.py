import os
import boto3
import re
from typing import Optional
from dotenv import load_dotenv
import sys


load_dotenv()


s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

bucket_name: str = "developer-task"
prefix: str = "a-wing/"


def list_files_in_bucket() -> None:
    """Lists all files in the S3 bucket with the specified prefix."""
    try:
        response: Optional[dict] = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if response and 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("No files found in the folder.")
    except Exception as e:
        print(f"Error while listing files: {e}")

# Function to upload a local file to S3
def upload_file(file_path: str, s3_key: str) -> None:
    """Uploads a local file to the specified location in S3."""
    try:
        s3.upload_file(file_path, bucket_name, f"{prefix}{s3_key}")
        print(f"File {file_path} has been uploaded to {prefix}{s3_key}")
    except Exception as e:
        print(f"Error while uploading file: {e}")

# Function to list files matching a regex
def list_files_with_regex(regex: str) -> None:
    """Lists files in S3 that match the given regular expression."""
    try:
        response: Optional[dict] = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        pattern: re.Pattern = re.compile(regex)
        if response and 'Contents' in response:
            for obj in response['Contents']:
                if pattern.match(obj['Key']):
                    print(obj['Key'])
        else:
            print("No files found in the folder.")
    except Exception as e:
        print(f"Error while listing files with regex: {e}")


def delete_files_with_regex(regex: str) -> None:
    """Deletes files in S3 that match the given regular expression."""
    try:
        response: Optional[dict] = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        pattern: re.Pattern = re.compile(regex)
        if response and 'Contents' in response:
            for obj in response['Contents']:
                if pattern.match(obj['Key']):
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    print(f"File {obj['Key']} has been deleted.")
        else:
            print("No files to delete.")
    except Exception as e:
        print(f"Error while deleting files: {e}")


def main() -> None:
    """Main CLI function for managing files in S3."""
    if len(sys.argv) < 2:
        print("Please specify an action: list, upload, list_regex, delete_regex")
        return

    action: str = sys.argv[1]

    if action == "list":
        list_files_in_bucket()
    elif action == "upload":
        if len(sys.argv) < 4:
            print("Usage: upload <file_path> <s3_key>")
        else:
            file_path: str = sys.argv[2]
            s3_key: str = sys.argv[3]
            upload_file(file_path, s3_key)
    elif action == "list_regex":
        if len(sys.argv) < 3:
            print("Usage: list_regex <regex>")
        else:
            regex: str = sys.argv[2]
            list_files_with_regex(regex)
    elif action == "delete_regex":
        if len(sys.argv) < 3:
            print("Usage: delete_regex <regex>")
        else:
            regex: str = sys.argv[2]
            delete_files_with_regex(regex)
    else:
        print("Unknown action. Available actions: list, upload, list_regex, delete_regex")

if __name__ == "__main__":
    main()
