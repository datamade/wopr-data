#  0: WBAN
#  1: Date
#  2: Time
#  3: StationType
#  4: SkyCondition
#  5: SkyConditionFlag
#  6: Visibility
#  7: VisibilityFlag
#  8: WeatherType
#  9: WeatherTypeFlag
# 10: DryBulbFarenheit
# 11: DryBulbFarenheitFlag
# 12: DryBulbCelsius
# 13: DryBulbCelsiusFlag
# 14: WetBulbFarenheit
# 15: WetBulbFarenheitFlag
# 16: WetBulbCelsius
# 17: WetBulbCelsiusFlag
# 18: DewPointFarenheit
# 19: DewPointFarenheitFlag
# 20: DewPointCelsius
# 21: DewPointCelsiusFlag
# 22: RelativeHumidity
# 23: RelativeHumidityFlag
# 24: WindSpeed
# 25: WindSpeedFlag
# 26: WindDirection
# 27: WindDirectionFlag
# 28: ValueForWindCharacter
# 29: ValueForWindCharacterFlag
# 30: StationPressure
# 31: StationPressureFlag
# 32: PressureTendency
# 33: PressureTendencyFlag
# 34: PressureChange
# 35: PressureChangeFlag
# 36: SeaLevelPressure
# 37: SeaLevelPressureFlag
# 38: RecordType
# 39: RecordTypeFlag
# 40: HourlyPrecip
# 41: HourlyPrecipFlag
# 42: Altimeter
# 43: AltimeterFlag

#  0: Wban Number => 0
#  1: YearMonthDay => 1
#  2: Time => 2
#  3: Station Type => 3
#  4: Maintenance Indicator => null
#  5: Sky Conditions => 4
#  6: Visibility => 6
#  7: Weather Type => 8
#  8: Dry Bulb Temp => 10
#  9: Dew Point Temp => 18
# 10: Wet Bulb Temp => 14
# 11: % Relative Humidity => 22
# 12: Wind Speed (kt) => 24
# 13: Wind Direction => 26
# 14: Wind Char. Gusts (kt) => null
# 15: Val for Wind Char. => 28
# 16: Station Pressure => 30
# 17: Pressure Tendency => 32
# 18: Sea Level Pressure => 36
# 19: Record Type => 38
# 20: Precip. Total => 40

import csv
import sys
from csvkit.cli import parse_column_identifiers

HEADER = ['wban','station_type','skycondition','skyconditionflag','visibility',
    'visibilityflag','weathertype','weathertypeflag','drybulbfarenheit','drybulbfarenheitflag',
    'drybulbcelsius','drybulbcelsiusflag','wetbulbfarenheit','wetbulbfarenheitflag',
    'wetbulbcelsius','wetbulbcelsiusflag','dewpointfarenheit','dewpointfarenheitflag',
    'dewpointcelsius','dewpointcelsiusflag','relativehumidity','relativehumidityflag',
    'windspeed','windspeedflag','winddirection','winddirectionflag','valueforwindcharacter',
    'valueforwindcharacterflag','stationpressure','stationpressureflag','pressuretendency',
    'pressuretendencyflag','pressurechange','pressurechangeflag','sealevelpressure',
    'sealevelpressureflag','recordtype','recordtypeflag','hourlyprecip','hourlyprecipflag',
    'altimeter','altimeterflag','date_time',
]

def process_old_file(reader):
    rows = [HEADER]
    cols = [0,3,5,42,6,42,7,42,8,42,42,42,10,42,42,42,9,42,42,42,11,42,12,42,13,42,15,
            42,16,42,17,42,42,42,18,42,19,42,20,42,42,21]
    raw_rows = []
    for row in reader:
        for i in range(len(HEADER) - len(row)):
            row.append('')
        raw_rows.append(row)
    for row in raw_rows:
        rows.append([row[c] for c in cols])
    return rows

def process_new_file(reader):
    rows = [HEADER]
    cols = [0,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,
            30,31,32,33,34,35,36,37,38,39,40,41,42,43,44]
    for row in reader:
        val = []
        for col in cols:
            val.append(row[col])
        rows.append(val)
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
