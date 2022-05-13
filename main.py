from fastapi import FastAPI, UploadFile, File, HTTPException
import uvicorn
import psycopg2
import glob
import shutil
import os
from decouple import config
import boto3

AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')

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


    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    s3.Bucket('assamtenders').upload_file(Filename=file_path, Key=str(file_name)+'.csv')
    os.remove(file_path)

    # Response
    return {"file name": file.filename}


if __name__ == '__main__':
    uvicorn.run(app)