from prefect import task, Flow, Parameter
import os
from decouple import config
import boto3
import pandas as pd
import pandas.io.sql as psql
import psycopg2
from io import BytesIO
import numpy as np
from fuzzywuzzy import fuzz




DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')


AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')

@task
def extract_from_s3(s3_key):
    #Extract raw file in AWS S3 Bucket
    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    obj = s3.Object('assamtenders', s3_key)
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)

    return df

@task(nout=4)
def transform_into_schema(df):
    # Schema Design:
    # 1NF Remove Duplicates and multivalued columns (if any)
    df = df.drop_duplicates(subset='ocid').reset_index(drop=True)
    # 2NF and 3NF are satisfied -- there are no partial, transitive dependencies.

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

    df_static = df.drop(['tender_stage', 'tender_status'], axis=1)
    cols_table_1 = ', '.join(
        "{} {}".format(n, d) for (n, d) in zip(df_static.columns, df_static.dtypes.astype(str).replace(psql_types)))

    df_updates = df[['ocid', 'date', 'tender_stage', 'tender_status']]
    cols_table_2 = ', '.join(
        "{} {}".format(n, d) for (n, d) in zip(df_updates.columns, df_updates.dtypes.astype(str).replace(psql_types)))
    return df_static, cols_table_1, df_updates, cols_table_2

@task
def load_to_db_static(df_static,cols_table_1):
    schema_name = 'assam_procurements'
    table1_name = 'tenders_static'  # For fields that would be written only once.

    #Connect to db
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()

    # Create Schema in DB -- TRANSACTION 1
    create_schema = '''CREATE SCHEMA IF NOT EXISTS {}'''.format(schema_name)
    cursor.execute(create_schema)
    conn.commit()


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
    # Finally remove files from server
    os.remove('data.csv')

    return None

@task
def load_to_db_updates(df_updates, cols_table_2):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    schema_name = 'assam_procurements'
    table1_name = 'tenders_static'  # For fields that would be written only once.
    table2_name = 'tenders_update'  # For fields that would see constant updates. (tender_status etc)

    # CREATE TABLE2 on DB -- TRANSACTION 4
    cursor.execute("SET search_path TO {}".format(schema_name))
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
    conn.close()
    #Finally remove files from server
    os.remove('data.csv')

    # Response
    return None

@task
def identify_flood_tenders():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")
    cursor.execute("DROP TABLE IF EXISTS tenders_flood;")
    conn.commit()

    # Keywords to identify flood related tenders
    positive_kw = ['Flood', 'Embankment', 'embkt', 'Relief', 'Erosion', 'SDRF', 'River', 'Inundation', 'Hydrology',
                   'Silt', 'Siltation', 'Bund', 'Trench', 'Drain', 'Culvert', 'Sluice', 'Bridge', 'Dyke',
                   'Storm water drain']
    negative_kw = ['Driver', 'Floodlight', 'Flood Light']
    positive_kw = [kw.lower() for kw in positive_kw]
    negative_kw = [kw.lower() for kw in negative_kw]

    # Read table created in the above task
    df = psql.read_sql('''SELECT ocid, tender_title, tender_externalreference FROM tenders_static;''', conn)
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

    return flood_df

@task
def create_tender_flood_table(flood_df):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")
    cursor.execute("DROP TABLE IF EXISTS tenders_flood;")
    conn.commit()

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

    # Remove files on server
    os.remove('data.csv')
    conn.close()
    return None

@task
def standardise_river_names():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")

    flood_df = psql.read_sql('''SELECT tenders_flood.ocid, tender_title FROM tenders_static
                                INNER JOIN tenders_flood
                                ON tenders_static.ocid=tenders_flood.ocid;''', conn)
    river_df = flood_df[
        (flood_df['tender_title'].str.contains('River')) | (flood_df['tender_title'].str.contains('river'))]
    rivers = []
    for title in river_df['tender_title']:
        title = title.replace(',', ' ').replace('_', ' '). \
            replace('.', ' ').replace('ofriver', 'river'). \
            replace('Riverbank', 'river').replace('-', ' ').replace('RIVER', 'river').replace('River', 'river')

        if 'river' in title:
            idx = title.split().index('river')
            prefix = title.split()[idx - 1]

            try:
                suffix = title.split()[idx + 1]
            except:
                suffix = 'River'  # This is manually removed later

            if prefix in ['samoka']:
                river = prefix
            elif suffix in ['brahmaputra', 'kollong']:
                river = suffix
            # These were the only rivers with lower case initials.
            elif (suffix[0].isupper()) & (len(suffix) >= 4):
                river = suffix
            elif (prefix[0].isupper()) & (len(prefix) >= 4):
                river = prefix
            else:
                river = None

        rivers.append(river)

    r1 = list(set(rivers).copy() - set([None]))
    r1.sort(reverse=True)
    r2 = list(set(rivers).copy() - set([None]))
    r2.sort()

    def remove_spelling_mistakes(r1, r2):
        for i in r1:
            scores = []
            for j in r2:
                scores.append(fuzz.ratio(i, j))
            while max(scores) == 100:
                scores[scores.index(100)] = -1

            if max(scores) >= 80:
                change = r2[scores.index(max(scores))]
                to_delete = r1[r1.index(i)]
                r1[r1.index(i)] = change
                r2[r2.index(to_delete)] = change
        return r1, r2

    k = True
    elbow = []
    while k:
        r1, r2 = remove_spelling_mistakes(r1, r2)
        elbow.append(len(set(r2)))
        if (len(elbow) >= 2):
            if (elbow[-1] == elbow[-2]):
                k = False
    # Elbow reached in two iterations.

    # Manually removing remaining bad elements
    river_names_std = set(r1) - set(['Bank', 'Training', 'Erosion', 'Front', 'Course', 'District', 'River', 'Embankment'])
    return river_names_std

@task
def create_assam_rivers_table(river_names_std):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")

    river_df = psql.read_sql('''SELECT river_name FROM assam_rivers''', conn)
    river_names = river_df.river_name.to_list()

    river_names_std = list(set(river_names_std)-set(river_names))


    # CREATE TABLE for rivers
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS assam_procurements.assam_rivers (id serial PRIMARY KEY, river_name varchar);")
    for river in list(river_names_std):
        cursor.execute("INSERT INTO assam_procurements.assam_rivers(river_name) VALUES ('{}');".format(str(river)))
    conn.commit()

@task
def identify_river_from_title():
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

    assam_rivers = psql.read_sql('''SELECT * FROM assam_procurements.assam_rivers;''', conn)
    l = list(assam_rivers.id)

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
    tender_river_df = flood_df
    return tender_river_df

@task
def create_tender_river_table(tender_river_df):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO assam_procurements")

    create_table = '''CREATE TABLE IF NOT EXISTS assam_procurements.tender_river 
                            (id SERIAL PRIMARY KEY,
                            ocid varchar, river_id int,
                            FOREIGN KEY (ocid) REFERENCES tenders_static(ocid),
                            FOREIGN KEY (river_id) REFERENCES assam_rivers(id));'''
    cursor.execute(create_table)
    conn.commit()

    tender_river_df.to_csv('data.csv', index=False, header=tender_river_df.columns, encoding='utf-8')
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

    return None

with Flow('my_etl') as flow:
    s3_key = Parameter(required=True,name='s3_key')
    df = extract_from_s3(s3_key)
    df_static, cols_table_1, df_updates, cols_table_2 = transform_into_schema(df)
    intermediate1 = load_to_db_static(df_static,cols_table_1)
    intermediate2 = load_to_db_updates(df_updates, cols_table_2)
    flood_df = identify_flood_tenders(upstream_tasks=[intermediate1])
    intermediate3 = create_tender_flood_table(flood_df)
    river_names_std = standardise_river_names(upstream_tasks=[intermediate3])
    intermediate4 = create_assam_rivers_table(river_names_std)
    tender_river_df = identify_river_from_title(upstream_tasks=[intermediate4])
    create_tender_river_table(tender_river_df)


#flow.visualize()

#flow.run(parameters={'s3_key':'CivicDataLab_ Assam Public Procurement Data _ #not-to-be-shared - ocds_mapped_compiled.csv'})

flow.register()