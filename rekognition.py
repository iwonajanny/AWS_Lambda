import os
import boto3

s3 = boto3.resource('s3')
rekognition = boto3.client('rekognition')


def lambda_handler(event, context):
    
    #get object
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    #read object 
    obj = s3.Object(bucket, key)
    image = obj.get()['Body'].read()
    
    #invoke Rekognition service
    resp = rekognition.recognize_celebrities(Image={'Bytes': image})
    
    #response
    names = []
    for celebrity in resp['CelebrityFaces']:
        name = celebrity['Name']
        names.append(name)
    
    print(names)