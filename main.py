from fastapi import FastAPI, UploadFile, File, HTTPException
import uvicorn
import psycopg2
import glob
import shutil
import os
from decouple import config

DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')

cwd = os.getcwd()
# Begin API
app = FastAPI()

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    file_name = file.filename
    file_name = file_name[-10:]
    print('file')
    file_path = r'Uploads/' + file_name

    # Save the uploaded file on server
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        await file.close()


    file_path = cwd+r'/'+file_path
    file_path = file_path.replace('\\','/')
    print(file_path)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()
    cur.execute("SET search_path TO assignments")
    sql_upload = '''
        INSERT INTO assignments.raw_csv(csv_file,added_on) 
        VALUES (lo_import('{}'),
    		'2022-05-13');
    		'''.format(file_path)


    cur.execute(sql_upload)
    conn.commit()
    os.remove(file_path)

    # Response
    return {"file name": file.filename}


if __name__ == '__main__':
    uvicorn.run(app)