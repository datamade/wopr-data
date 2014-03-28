# HOURLY_HEADER="wban_number,date_time,station_type,maintenance_indicator,\
# sky_conditions,visibility,weather_type,dry_bulb_temp,dew_point_temp,\
# wet_bulb_temp,relative_humidity,wind_speed_kt,wind_direction,\
# wind_char_gusts_kt,val_for_wind_char,station_pressure,pressure_tendency,\
# sea_level_pressure,record_type,precip_total"
#
import csv
import sys
from csvkit.cli import parse_column_identifiers

HEADER = ['wban','date_time','station_type','skycondition','skyconditionflag','visibility',
    'visibilityflag','weathertype','weathertypeflag','drybulbfarenheit','drybulbfarenheitflag',
    'drybulbcelsius','drybulbcelsiusflag','wetbulbfarenheit','wetbulbfarenheitflag',
    'wetbulbcelsius','wetbulbcelsiusflag','dewpointfarenheit','dewpointfarenheitflag',
    'dewpointcelsius','dewpointcelsiusflag','relativehumidity','relativehumidityflag',
    'windspeed','windspeedflag','winddirection','winddirectionflag','valueforwindcharacter',
    'valueforwindcharacterflag','stationpressure','stationpressureflag','pressuretendency',
    'pressuretendencyflag','pressurechange','pressurechangeflag','sealevelpressure',
    'sealevelpressureflag','recordtype','recordtypeflag','hourlyprecip','hourlyprecipflag',
    'altimeter','altimeterflag'
]

def process_old_file(reader):
    rows = [HEADER]
    for row in reader:
        for i in range(len(HEADER) - len(row)):
            row.append('')
        rows.append(row)
    return rows

def process_new_file():
    rows = []
    return rows

if __name__ == "__main__":
    reader = csv.reader(sys.stdin)
    header = reader.next()
    if len(header) <= 22:
        rows = process_old_file(reader)
    else:
        rows = process_new_file(reader)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
