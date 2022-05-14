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


    # Upload to database:

    #Connect to db
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()

    # Import the required file to be loaded.
    df = pd.read_csv(file_path)

    # Schema Design:
    # 1NF Remove Duplicates and multivalued columns (if any)
    df = df.drop_duplicates(subset='ocid').reset_index(drop=True)
    # 2NF and 3NF are satisfied -- there are no partial, transitive dependencies.

    schema_name = 'assam_procurements'
    table1_name = 'tenders_static'  # For fields that would be written only once.
    table2_name = 'tenders_update'  # For fields that would see constant updates. (tender_status etc)

    # Clean column headers
    df.columns = [column_name.lower().replace(" ", "_").replace(r'/', '_') for column_name in df.columns]

    # Clean column types and map to POSTGRES types
    df.date = pd.to_datetime(df.date)
    df.tender_bidopening_date = pd.to_datetime(df.tender_bidopening_date)
    df.tender_milestones = pd.to_datetime(df.tender_milestones)
    df.tender_milestones_duedate = pd.to_datetime(df.tender_milestones_duedate)
    df.tender_datepublished = pd.to_datetime(df.tender_datepublished)

    psql_types = {'object': 'varchar',
                  'int64': 'int',
                  'datetime64[ns, UTC]': 'timestamp',
                  'datetime64[ns]': 'timestamp'}

    # Create Schema in DB -- TRANSACTION 1
    create_schema = '''CREATE SCHEMA IF NOT EXISTS {}'''.format(schema_name)
    cursor.execute(create_schema)
    conn.commit()

    # Table 1 - Static
    df_static = df.drop(['tender_stage','tender_status'],axis=1)
    cols_table_1 = ', '.join("{} {}".format(n, d) for (n, d) in zip(df_static.columns, df_static.dtypes.astype(str).replace(psql_types)))

    # CREATE TABLE1 on DB -- TRANSACTION 2
    cursor.execute("SET search_path TO {}".format(schema_name))
    create_table = '''CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY (ocid));'''.format(table1_name, cols_table_1)
    cursor.execute(create_table)
    cursor.execute('''ALTER TABLE {} ALTER COLUMN tender_value_amount TYPE numeric'''.format(table1_name))
    conn.commit()

    # insert values to table1 -- TRANSACTION 3
    cursor.execute("SET search_path TO {}".format(schema_name))
    dataframe = psql.read_sql('''SELECT ocid FROM {};'''.format(table1_name), conn)
    merged = df_static.merge(dataframe, on='ocid', how='left', indicator=True)
    merged = merged[merged["_merge"] == 'left_only'].drop('_merge', axis=1)
    merged.to_csv('data.csv', index=False, header=merged.columns, encoding='utf-8')
    data = open('data.csv')
    insert = ''' COPY {} FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ',';
                '''.format(table1_name)
    cursor.copy_expert(sql=insert, file=data)
    conn.commit()
    data.close()

    # CREATE TABLE2 on DB -- TRANSACTION 4
    cursor.execute("SET search_path TO {}".format(schema_name))
    df_updates = df[['ocid', 'date', 'tender_stage', 'tender_status']]
    cols_table_2 = ', '.join(
        "{} {}".format(n, d) for (n, d) in zip(df_updates.columns, df_updates.dtypes.astype(str).replace(psql_types)))
    create_table = '''CREATE TABLE IF NOT EXISTS {} ({}, FOREIGN KEY (ocid) REFERENCES {}(ocid));'''.format(table2_name,
                                                                                                            cols_table_2,
                                                                                                            table1_name)
    cursor.execute(create_table)
    conn.commit()

    # insert values to table2 -- TRANSACTION 5
    cursor.execute("SET search_path TO {}".format(schema_name))
    dataframe = psql.read_sql('''SELECT ocid, date FROM {};'''.format(table2_name), conn)
    dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True)
    merged = df_updates.merge(dataframe.head(), how='left', on=['ocid', 'date'], indicator=True)
    merged = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
    merged.to_csv('data.csv', index=False, header=df_updates.columns, encoding='utf-8')

    data = open('data.csv')

    insert = ''' COPY {} FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ',';
                '''.format(table2_name)

    cursor.copy_expert(sql=insert, file=data)
    conn.commit()
    data.close()

    #Finally remove files from server
    os.remove(file_path)
    conn.close()

    os.remove('data.csv')

    # Response
    return {"file name": file.filename}

@app.get("/identify_flood_tenders")
def identify_flood_tenders(positive_kw: List[str] = Query(pos_kw),
                           negative_kw: List[str] = Query(neg_kw)
                           ):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")
    cursor.execute("DROP TABLE IF EXISTS tenders_flood;")
    conn.commit()

    # Keywords to identify flood related tenders

    positive_kw = [kw.lower() for kw in positive_kw]
    negative_kw = [kw.lower() for kw in negative_kw]

    df = psql.read_sql('''SELECT ocid, tender_title, tender_externalreference FROM tenders_static;''', conn)

    print(df.shape)
    # Logic for filtering flood related tenders based on title and reference
    df['flood_related_bool1'] = df['tender_title'].map(
        lambda x: max([word.lower() in positive_kw for word in x.split()]))
    df['flood_related_bool2'] = df['tender_externalreference'].map(
        lambda x: max([word.lower() in positive_kw for word in x.split()]))

    df['not_flood_related_bool1'] = df['tender_title'].map(
        lambda x: max([word.lower() in negative_kw for word in x.split()]))
    df['not_flood_related_bool2'] = df['tender_externalreference'].map(
        lambda x: max([word.lower() in negative_kw for word in x.split()]))

    df['flood_related_bool'] = (df['flood_related_bool1']) | (df['flood_related_bool2'])
    df.drop(['flood_related_bool1', 'flood_related_bool2'], axis=1, inplace=True)

    df['not_flood_related_bool'] = (df['not_flood_related_bool1']) | (df['not_flood_related_bool2'])
    df.drop(['not_flood_related_bool1', 'not_flood_related_bool2'], axis=1, inplace=True)

    flood_df = df[(df['flood_related_bool']) & (~df['not_flood_related_bool'])]
    flood_df = flood_df[['ocid']]
    print(flood_df.shape)



    ## CREATE FLOOD TENDERS TABLE -- TRANSACTION 1
    create_table = '''CREATE TABLE IF NOT EXISTS tenders_flood (id SERIAL PRIMARY KEY,ocid varchar,
                      FOREIGN KEY (ocid) REFERENCES tenders_static(ocid));'''
    cursor.execute(create_table)
    conn.commit()

    # insert new values to tender_floods table -- TRANSACTION 2

    flood_df.to_csv('data.csv', index=False, header=flood_df.columns, encoding='utf-8')
    data = open('data.csv')

    insert = ''' COPY tenders_flood(ocid) FROM STDIN WITH
                    CSV
                    HEADER
                    DELIMITER AS ',';
                    '''
    cursor.copy_expert(sql=insert, file=data)
    conn.commit()
    data.close()

    #Remove files on server
    os.remove('data.csv')
    conn.close()

    # Response
    return {"Status": 'Flood related tenders identified in tender_floods table'}

@app.get("/identify_rivers")
def identify_rivers():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")
    cursor.execute("DROP TABLE IF EXISTS tender_river;")
    conn.commit()

    river_ids = []
    flood_df = psql.read_sql('''SELECT tenders_flood.ocid, tender_title
                                FROM assam_procurements.tenders_flood
                                INNER JOIN assam_procurements.tenders_static
                                ON tenders_static.ocid = tenders_flood.ocid;''', conn)

    for k in flood_df['tender_title']:
        words = k.replace(',', ' ').replace('_', ' '). \
            replace('.', ' ').replace('ofriver', 'river'). \
            replace('Riverbank', 'river').replace('-', ' ').replace('RIVER', 'river').replace('River', 'river').split()
        try:
            idx = words.index('river')
        except:
            river_ids.append(None)
            continue
        prefix = words[idx - 1]
        try:
            suffix = words[idx + 1]
        except:
            suffix = None

        scores_p = []
        scores_s = []
        assam_rivers = psql.read_sql('''SELECT * FROM assam_procurements.assam_rivers;''', conn)

        l = list(assam_rivers.id)

        for j in list(assam_rivers.river_name):
            scores_p.append(fuzz.ratio(j, prefix))
            scores_s.append(fuzz.ratio(j, suffix))

        if max(scores_p) >= max(scores_s):
            river_id = l[scores_p.index(max(scores_p))]
        elif max(scores_p) < max(scores_s):
            river_id = l[scores_s.index(max(scores_s))]
        else:
            river_id = None

        river_ids.append(river_id)
    flood_df['river_id'] = river_ids
    flood_df['river_id'] = flood_df['river_id']

    flood_df = flood_df.drop(['tender_title'], axis=1)
    flood_df = flood_df.dropna()
    flood_df['river_id'] = flood_df['river_id'].astype('int')

    create_table = '''CREATE TABLE IF NOT EXISTS assam_procurements.tender_river 
                        (id SERIAL PRIMARY KEY,
                        ocid varchar, river_id int,
                        FOREIGN KEY (ocid) REFERENCES tenders_static(ocid),
                        FOREIGN KEY (river_id) REFERENCES assam_rivers(id));'''
    cursor.execute(create_table)
    conn.commit()

    flood_df.to_csv('data.csv', index=False, header=flood_df.columns, encoding='utf-8')
    data = open('data.csv')

    insert = ''' COPY tender_river(ocid, river_id) FROM STDIN WITH
                        CSV
                        HEADER
                        DELIMITER AS ',';
                        '''
    cursor.copy_expert(sql=insert, file=data)
    conn.commit()
    data.close()

    # Remove files on server
    os.remove('data.csv')
    conn.close()

    # Response
    return {"Status": 'Rivers identified in tender_river table'}


if __name__ == '__main__':
    uvicorn.run(app)