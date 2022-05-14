import psycopg2
from decouple import config
import pandas.io.sql as psql
import numpy as np
from fuzzywuzzy import fuzz


DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=5432)
cursor = conn.cursor()

cursor.execute("SET search_path TO assam_procurements")


flood_df = psql.read_sql('''SELECT tenders_flood.ocid, tender_title FROM tenders_static
                            INNER JOIN tenders_flood
                            ON tenders_static.ocid=tenders_flood.ocid;''', conn)
river_df = flood_df[(flood_df['tender_title'].str.contains('River'))|(flood_df['tender_title'].str.contains('river'))]

rivers = []
for title in river_df['tender_title']:
    title = title.replace(',', ' ').replace('_', ' ').\
        replace('.', ' ').replace('ofriver', 'river').\
        replace('Riverbank','river').replace('-',' ').replace('RIVER','river').replace('River','river')

    if 'river' in title:
        idx = title.split().index('river')
        prefix = title.split()[idx - 1]

        try:
            suffix = title.split()[idx + 1]
        except:
            suffix = 'River' #This is manually removed later

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
            river = np.NaN

    rivers.append(river)

r1 = list(set(rivers).copy() - set([np.NaN]))
r1.sort(reverse=True)
r2 = list(set(rivers).copy() - set([np.NaN]))
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
    r1,r2 = remove_spelling_mistakes(r1,r2)
    elbow.append(len(set(r2)))
    if (len(elbow)>=2):
        if (elbow[-1]==elbow[-2]):
            k=False
# Elbow reached in two iterations.

# Manually removing remaining bad elements
rivers = set(r1)-set(['Bank','Training','Erosion','Front','Course','District','River','Embankment'])

# CREATE TABLE for rivers
cursor.execute("CREATE TABLE IF NOT EXISTS assam_procurements.assam_rivers (id serial PRIMARY KEY, river_name varchar);")
for river in list(rivers):
    cursor.execute("INSERT INTO assam_procurements.assam_rivers(river_name) VALUES ('{}');".format(str(river)))
conn.commit()
