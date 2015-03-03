from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db
from spandex.utils import load_config
import os
import psycopg2
from six.moves import cStringIO

loader = TableLoader()

config = load_config()
db_config = dict(config.items('database'))

parcel_postgres_backup = loader.get_path('out/parcel.backup')
os.system('pg_dump --host %s --port %s --username "%s" --format custom --verbose --file "%s" --table "public.parcel" "mtc"' % (db_config['host'], db_config['port'], db_config['user'], parcel_postgres_backup)) 

buildings = db_to_df('select * from buildings').set_index('building_id')

def exec_sql2(query):
    print query
    conn_string = "host=67.225.185.54 dbname='mtc' user='urbancanvas' password='****' port=5432"
    import psycopg2
    conn=psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    
exec_sql2("CREATE EXTENSION IF NOT EXISTS postgis;")

exec_sql2("drop table if exists parcel;")

os.system('pg_restore --host 67.225.185.54 --port 5432 --username "urbancanvas" --dbname "mtc" --role "urbancanvas" --no-password  --verbose "%s"' % parcel_postgres_backup)

exec_sql2("drop table if exists building;")

urbancanvas_conn=psycopg2.connect("host=67.225.185.54 dbname='mtc' user='urbancanvas' password='****' port=5432")
cur = urbancanvas_conn.cursor()
df = buildings
schema_name = 'public'
table_name = 'building'
df.columns = [s.lower() for s in df.columns]
empty_df = df.iloc[[0]]

engine = create_engine('postgresql://', creator=lambda: urbancanvas_conn)
empty_df.to_sql(table_name, engine, schema=schema_name,
                        index=True, if_exists='replace')
cur.execute("DELETE FROM {}".format(table_name))
buf = cStringIO()
df.to_csv(buf, sep='\t', na_rep=r'\N', header=False, index=True)
buf.seek(0)
cur.copy_from(buf, table_name,
              columns=tuple([df.index.name] +
                            df.columns.values.tolist()))
cur.close()
urbancanvas_conn.close()