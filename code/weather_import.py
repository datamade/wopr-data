import ftplib
from cStringIO import StringIO
import csv
import gzip
from datetime import datetime
import os
from collections import OrderedDict
from weather_lookups import ADD_DATA

FTP_SERVER = 'ftp.ncdc.noaa.gov'
DATA_DIR = '/pub/data/noaa/'
FTP_USER, FTP_PW = ('ftp', 'info@datamade.us')

def parse_control_data(data):
    cd = {}
    cd['add_data_len'] = data[:4]
    cd['usaf_code'] = data[4:10]
    cd['wban_code'] = data[10:15]
    cd['date'] = data[15:23]
    cd['time'] = data[23:27]
    cd['source_code'] = data[27]
    cd['latitude'] = float(data[28:34]) / 1000
    cd['longitude'] = float(data[34:41]) / 1000
    cd['report_type'] = data[41:46]
    cd['elevation'] = data[46:51]
    cd['station_call_id'] = data[51:56]
    cd['qc_process'] = data[56:60]
    return cd

def parse_mandatory_data(data):
    md = {}
    md['wind_direction_angle'] = data[60:63]
    md['wind_direction_qc'] = data[63]
    md['wind_type_code'] = data[64]
    md['wind_speed'] = data[65:69]
    md['wind_speed_qc'] = data[69]
    md['sky_condition'] = data[70:75]
    md['sky_observation_qc'] = data[75]
    md['sky_determination_code'] = data[76]
    md['sky_condition_cavok'] = data[77]
    md['vis_distance'] = data[78:84]
    md['vis_distnace_qc'] = data[84]
    md['vis_variability_code'] = data[85]
    md['vis_observation_qc'] = data[86]
    md['temp_celsius'] = data[87:92]
    md['temp_observation_qc'] = data[92]
    md['dew_point'] = data[93:98]
    md['dew_point_qc'] = data[98]
    md['pressure'] = data[99:104]
    md['pressure_qc'] = data[104]
    return md

def parse_additional_data(data):
    try:
        remarks_idx = data.index('REM')
        data = data[108:remarks_idx]
    except ValueError:
        data = data[108:]
    idx = 0
    ad = {}
   #while idx < len(data):
   #    mapper = ADD_DATA.get(obs_indicator)
   #    if mapper:
   #        for row in mapper['mapping']:
   #            try:
   #                field, start, end = row
   #                ad[field] = data[(idx + start):(idx + end)]
   #            except ValueError:
   #                field, start = row
   #                ad[field] = data[(idx + start)]
   #        width = mapper['width']
   #        idx = idx + width
    return data


def fetch_station_data(info, end=datetime.now().year, begin=2001, state='IL'):
    reader = csv.DictReader(info)
    rows = []
    for row in reader:
        try:
            row['BEGIN'] = int(row['BEGIN'][:4])
            row['END'] = int(row['END'][:4])
        except ValueError:
            continue
        if row['END'] == end and row['BEGIN'] >= begin and row['STATE'] == state:
            yield row

def fetch_observations(row):
    fname = '%s-%s-%s.gz' % (row['USAF'], row['WBAN'], row['END'])
    fpath = 'downloads/%s/%s' % (row['END'], fname)
    if not os.path.exists(fpath):
        ftp = ftplib.FTP(FTP_SERVER)
        ftp.login(FTP_USER, FTP_PW)
        ftp.cwd('%s%s' % (DATA_DIR, row['END']))
        try:
            os.mkdir('downloads/%s' % row['END'])
        except:
            pass
        ftp.retrbinary('RETR %s' % (fname), open(fpath, 'wb').write)
    fileobj = open(fpath, 'rb')
    with gzip.GzipFile(fileobj=fileobj) as gz:
        for line in gz:
            observation = {}
            # Check if this line has the correct observation type:
            # ASOS/AWOS observation merged with USAF SURFACE HOURLY observation
            if line[27] == '7':
                #observation.update(parse_control_data(line))
                #observation.update(parse_mandatory_data(line))
                #observation.update(parse_additional_data(line))
                ad = parse_additional_data(line)
                if ad:
                    yield ad
                #yield observation

if __name__ == '__main__':
    history_path = 'downloads/ish-history.csv'
    if not os.path.exists(history_path):
        ftp = ftplib.FTP(FTP_SERVER)
        ftp.login(FTP_USER, FTP_PW)
        ftp.cwd(DATA_DIR)
        ftp.retrbinary('RETR ish-history.csv', open(history_path, 'wb').write)
    history = open(history_path, 'rb')
    rows = fetch_station_data(history)
    for row in rows:
        observations = fetch_observations(row)
        for observation in observations:
            print observation

