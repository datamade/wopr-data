# https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
import psycopg2
import requests
import os
import csv
from cStringIO import StringIO
from datetime import datetime
from sqlalchemy import Table, create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from csvkit.sql import make_table, make_create_table_statement
from csvkit.table import Table as CSVTable

CHICAGO_ENDPOINT = 'https://data.cityofchicago.org/api/views'
VIEWS = {
    'chicago_crimes_all': 'ijzp-q8t2', 
    'chicago_business_licenses': 'r5kz-chrr',
}

DB_CONN = os.environ['WOPR_CONN']

# Maybe set an environmental variable?
DATA_DIR = 'data'

def load_and_save(dataset_name, dataset_id):
    now = datetime.now()
    fname = '%s_%s.csv' % (dataset_name, now.strftime('%Y-%m-%d'))
    fpath = '%s/%s' % (DATA_DIR, fname)
    if os.path.exists(fpath):
        f = open(fpath, 'rb')
        data = StringIO(f.read())
        print '%s already saved' % dataset_name
    else:
        print 'Downloading %s' % dataset_name
        r = requests.get('%s/%s/rows.csv?accessType=DOWNLOAD' % (CHICAGO_ENDPOINT, dataset_id))
        if r.status_code is 200:
            data = StringIO(r.content)
            outp = open(fpath, 'wb')
            outp.write(r.content)
            outp.close()
        else:
            # Raise an exception if the portal response is not 200
            raise
    reader = csv.reader(data)
    outp = StringIO()
    writer = csv.writer(outp)
    for i,v in enumerate(reader):
        if i > 100000:
            break
        else:
            writer.writerow(v)
    outp.seek(0)
    csv_table = CSVTable.from_csv(outp, name='src_%s' % dataset_name)
    sql_table = make_table(csv_table)
    create_statement = make_create_table_statement(sql_table)
    conn = psycopg2.connect(DB_CONN)
    cursor = conn.cursor()
    try:
        cursor.execute(create_statement)
        conn.commit()
    except psycopg2.ProgrammingError, e:
        conn.rollback()
        drop = 'DROP TABLE src_%s' % dataset_name
        cursor.execute(drop)
        cursor.execute(create_statement)
        conn.commit()
    copy_stmt = "COPY src_%s FROM STDIN WITH DELIMITER ',' CSV HEADER" % dataset_name
    cursor.copy_expert(sql=copy_stmt, file=data)
    conn.commit()
    data.close()
    print 'Saved %s' % fname
    return ''

def update(dataset_name):
    engine = create_engine(DB_CONN)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base = declarative_base()
    new_records = Table('new_%s' % dataset_name, Base.metadata,
        Column('LICENSE_ID', Integer, primary_key=True))
    print 'Made new records table'
    if new_records.exists(engine):
        new_records.drop(engine)
    new_records.create(engine)
    dat_table = Table('dat_%s' % dataset_name, Base.metadata,
        autoload=True, autoload_with=engine)
    src_table = Table('src_%s' % dataset_name, Base.metadata,
        autoload=True, autoload_with=engine)
    sel = select([src_table.c.license_id])\
        .select_from(
            src_table.outerjoin(
              dat_table, src_table.c.license_id == dat_table.c.license_id)
            )\
        .where(dat_table.c.chicago_business_licenses_row_id != None)
    new_records.insert().from_select(['LICENSE_ID'], sel)
    print 'Inserted new records into dat table'
    return None

if __name__ == '__main__':
    load_and_save('chicago_business_licenses', 'r5kz-chrr')
    # update('chicago_business_licenses')
