import boto3
import json


class Bucket:
    '''
        Class to interact with Amazon S3.
    '''
    def __init__(self, name):
            
            self.credentials = json.load(open('aws/credentials.json', 'r'))
            self.client = boto3.client('s3', aws_access_key_id=self.credentials['access_key'], aws_secret_access_key=self.credentials['access_secret'])
            self.name = name
            try:
                self.client.create_bucket(Bucket=self.name)
            except Exception as e:
                print(e)
  
    def put_file(self, key, data):
         '''
            Put an object in a bucket
         '''
         self.client.put_object(Bucket=self.name, Key=key, Body=data)

    def list_objects(self):
        objects_list = []
        objects = self.client.list_objects(Bucket=self.name)
        for object in objects['Contents']:
             objects_list.append(object['Key'])
        return objects_list
    
    def get_object(self, key):
        ext = key.split('.')[1]
        if ext == 'json':
            object = self.client.get_object(Bucket=self.name, Key=key)
            return json.loads(object['Body'].read().decode())
        
    def delete_object(self, key):
        self.client.delete_object(Bucket=self.name, Key=key)
        