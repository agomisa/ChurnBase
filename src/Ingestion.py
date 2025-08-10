import urllib3
import re
import boto3
import logging

# Logging settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-01/"
bucket_name = 'dest-wikimedia'

def lambda_handler(event, context):

    http = urllib3.PoolManager()
    response = http.request('GET', url)

    if response.status != 200:
        return {
            "statusCode": response.status,
            "body": f"Failed to fetch the page: HTTP {response.status}"
        }

    html = response.data.decode('utf-8')

    all_links = re.findall(r'href="([^"]+)"', html)
    non_gz_links = [link for link in all_links if not link.endswith('.gz') and link.startswith('projectviews')]
    total = len(non_gz_links)

    logger.info(f"Found {total} files on the page.")

    s3_client = boto3.client('s3')

    if total == 0:
        return {
            "statusCode": 404,
            "body": "No files found on the page."
        }
    
    for file in non_gz_links:

        logger.info(f"Found file: {file}")
        
        file_url = url + file

        print(f"File URL: {file_url}")

        key = f"projectviews/2025/2025-01/{file}"

        try:
            file_resp = http.request('GET', file_url, preload_content=False)
            logger.info(f"Downloading file from {file_url}")

            if file_resp.status != 200:
                file_resp.release_conn()
                return {
                    "statusCode": file_resp.status,
                    "body": f"Failed to download {file_url}: HTTP {file_resp.status}"
                }

            s3_client.upload_fileobj(file_resp, bucket_name, key)
            file_resp.release_conn()

            logger.info(f"Uploaded {file} to s3://{bucket_name}/{key}")

            return {
                "statusCode": 200,
                "body": f"File {file} uploaded to s3://{bucket_name}/{key}"
            }

        except Exception as e:
            
            logger.error(f"Error occurred: {str(e)}")
            
            return {
                "statusCode": 500,
                "body": f"Error: {str(e)}"
            }