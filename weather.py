# ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
# ftp.login('ftp', 'eric.vanzanten@gmail.com')
# ftp.cwd('/pub/data/asos-onemin/6406-2013/')
# ftp.retrbinary('RETR 64060KORD201305.dat', open('64060KORD201305.dat', 'wb').write)

# minute = datetime.strptime(line[13:25], '%Y%m%d%H%M')
# NP = No precipitation, R = Rain, S = Snow
# precip_type = line[31:33]
# pressure = line[69:75]
# temp = line[95:97]

import os
import re
import ftplib
from datetime import datetime, timedelta
import pymongo
c = pymongo.MongoClient()
db = c['chicago']
db.authenticate(os.environ['UPDATE_MONGO_USER'], os.environ['UPDATE_MONGO_PW'])
weather = db['weather_by_minute']
crime = db['crime']

PRECIP_LOOKUP = {
    'NP': None,
    'R': 'rain',
    'S': 'snow',
    'P': 'mixed',
    'M': None,
    'R+': 'rain',
    'R-': 'rain',
    'S+': 'snow',
    'S-': 'snow',
    '?2': None,
    '?3': None,
}

def download(ftp):
    for year in range(2001, 2014):
        ftp.cwd('/pub/data/asos-onemin/6406-%s/' % year)
        if year < 2013:
            for month in range(1, 13):
                month = str(month).zfill(2)
                fname = '64060KORD%s%s.dat' % (year, month)
                ftp.retrbinary('RETR %s' % fname, open('weather/%s' % fname, 'wb').write)
        else:
            for month in range(1,6):
                month = str(month).zfill(2)
                fname = '64060KORD%s%s.dat' % (year, month)
                ftp.retrbinary('RETR %s' % fname, open('weather/%s' % fname, 'wb').write)

def parseit(line):
    rex = re.compile(r'\s+')
    line = rex.sub(' ', line.replace('[', '').replace(']', ''))
    parts = line.split(' ')
    update = {}
    update['minute'] = datetime.strptime(parts[1][3:-4], '%Y%m%d%H%M')
    try:
        update['precip_type'] = PRECIP_LOOKUP[parts[2]]
    except KeyError:
        update['precip_type'] = None
    try:
        update['pressure'] = float(parts[-3])
    except ValueError:
        return None
    try:
        update['temp'] = int(parts[-2])
    except ValueError:
        return None
    return update

def loopit():
    for fil in os.listdir('weather'):
        lines = list(open('weather/%s' % fil, 'rb'))
        weather.insert([parseit(d) for d in lines if parseit(d)])
    weather.ensure_index([('minute', pymongo.DESCENDING)])

def matchit():
    for doc in crime.find():
      weat = weather.find_one({'minute': doc['date']})
      if not weat:
          weat = weather.find_one({'minute': {'$lte': doc['date'] + timedelta(minutes=5), '$gte': doc['date'] - timedelta(minutes=5)}})
      crime.update({'_id': doc['_id']}, {'$set': {'weather': weat}})

if __name__ == '__main__':
    # ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
    # ftp.login('ftp', 'eric.vanzanten@gmail.com')
    # download(ftp)
    loopit()
    matchit()
