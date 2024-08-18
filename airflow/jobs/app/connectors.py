import requests
import json
# import boto3
import os


class HttpConnector:
    def __init__(self, method: str='GET', url: str = None):
        self.method = method.upper()
        self.url = url
    
    def request_data(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }

        if self.url:
            if self.method == 'GET':
                response = requests.get(self.url, headers=headers)
                if response.status_code != 200: 
                    return ValueError('Error! Check if your api endpoint is correct...')
                return response.json()
            return ValueError("We're just working with get method at this moment")

class S3Connector:
    def __init__(self, local_path: str, landing_data: str, file_name: str):
        self.bucket_name = None
        self.s3_file_path = None
        self.local_path = local_path
        self.file_name = file_name
        self.landing_data = landing_data
    
    def landing_ingestion(self):
        s3_client = boto3.client()
        try:
            landing_data = json.dumps(self.landing_data)
            s3_client.put_object(Body=landing_data, Bucket=self.bucket_name, Key=self.s3_file_path)
        except Exception as err:
            ValueError(err)
    
    def landing_ingestion_local(self):
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        
        output_file = os.path.join(self.local_path, f"{self.file_name}.json")

        with open(output_file, "w") as f:
            
            json.dump(self.landing_data, f)