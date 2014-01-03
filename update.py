# https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
import psycopg2
import requests
import os
from cStringIO import StringIO
from datetime import datetime
from sqlalchemy import Table, create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

CHICAGO_ENDPOINT = 'https://data.cityofchicago.org/api/views'
VIEWS = {
    'chicago_crimes_all': 'ijzp-q8t2', 
    'chicago_business_licenses': 'r5kz-chrr',
}

DB_CONN = os.environ['WOPR_CONN']

# Maybe set an environmental variable?
DATA_DIR = '/tmp'

def load_and_save(dataset_name, dataset_id):
    conn = psycopg2.connect(DB_CONN)
    cursor = conn.cursor()
    r = requests.get('%s/%s/rows.csv?accessType=DOWNLOAD' % (CHICAGO_ENDPOINT, dataset_id))
    if r.status_code is 200:
        cursor.execute('DROP TABLE IF EXISTS SRC_%s', (dataset_name,))
        data = StringIO(r.content)
        cursor.copy_from(DATA_DIR, 'src_%s' % dataset_name)
        cursor.commit()
        now = datetime.now()
        outp = f.open('%s/%s_%s.csv' % (dataset_name, now.strftime('%Y-%m-%d')), 'wb')
        outp.write(r.content)
    else:
        # Raise an exception if the portal response is not 200
        raise
    return ''

def update(dataset_name):
    engine = create_engine(DB_CONN)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base = declarative_base()
    new_records = Table('new_%s' % dataset_name, Base.metadata,
        Column('LICENSE_ID', Integer, primary_key=True))
    if new_records.exists(engine):
        new_records.drop(engine)
    new_records.create(engine)
    sel = select([src_table.c.license_id])\
        .select_from(
            src_table.outerjoin(
              dat_table, src_table.c.license_id == dat_table.c.license_id)
            )\
        .where(dat_table.c.chicago_business_licenses_row_id != None)
    return None

if __name__ == '__main__':
    # load_and_save()
    update('chicago_business_licenses')
