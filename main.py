from fastapi import FastAPI, UploadFile, File, HTTPException, Query
import uvicorn
import psycopg2
import glob
import shutil
import os
from decouple import config
import boto3
import pandas as pd
import pandas.io.sql as psql
from typing import List
import numpy as np
from fuzzywuzzy import fuzz


AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')

pos_kw = ['Flood', 'Embankment', 'embkt', 'Relief', 'Erosion', 'SDRF', 'River', 'Inundation', 'Hydrology',
                   'Silt', 'Siltation', 'Bund', 'Trench', 'Drain', 'Culvert', 'Sluice', 'Bridge', 'Dyke',
                   'Storm water drain']
neg_kw = ['Driver', 'Floodlight', 'Flood Light']
cwd = os.getcwd()



# Begin API
app = FastAPI()

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    file_name = file.filename
    print('file')
    file_path = r'Uploads/' + file_name

    # Save the uploaded file on server
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        await file.close()


    file_path = cwd+r'/'+file_path
    file_path = file_path.replace('\\','/')
    print(file_path)

    #Save raw file in AWS S3 Bucket
    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    file_name_exists = True
    dup=1
    while file_name_exists:
        if file_name in [i.key for i in s3.Bucket('assamtenders').objects.all()]:
            file_name = file_name.split('.csv')[0]+str(dup)+'.csv'
            dup=dup+1
        else:
            file_name_exists = False

    s3.Bucket('assamtenders').upload_file(Filename=file_path, Key=str(file_name))

    # Response
    return {"File uploaded to S3"}

if __name__ == '__main__':
    uvicorn.run(app)