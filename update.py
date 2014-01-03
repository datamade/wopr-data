# https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
import psycopg2


CHICAGO_ENDPOINT = 'https://data.cityofchicago.org/api/views'
VIEWS = {
    'chicago_crimes_all': 'ijzp-q8t2', 
    'chicago_business_licenses': 'r5kz-chrr',
}

def make_it_so():
    return None

if __name__ == '__main__':
    make_it_so()
