MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
.SUFFIXES:
VPATH=raw:build
PG_HOST=localhost
PG_USER=postgres
PG_DB=wopr
PG_PORT=5432

HOURLY_HEADER="wban_number,date_time,station_type,maintenance_indicator,\
sky_conditions,visibility,weather_type,dry_bulb_temp,dew_point_temp,\
wet_bulb_temp,relative_humidity,wind_speed_kt,wind_direction,\
wind_char_gusts_kt,val_for_wind_char,station_pressure,pressure_tendency,\
sea_level_pressure,record_type,precip_total"

wban,date_time,station_type,skycondition,skyconditionflag,visibility,visibilityflag,weathertype,weathertypeflag,drybulbfarenheit,drybulbfarenheitflag,drybulbcelsius,drybulbcelsiusflag,wetbulbfarenheit,wetbulbfarenheitflag,wetbulbcelsius,wetbulbcelsiusflag,dewpointfarenheit,dewpointfarenheitflag,dewpointcelsius,dewpointcelsiusflag,relativehumidity,relativehumidityflag,windspeed,windspeedflag,winddirection,winddirectionflag,valueforwindcharacter,valueforwindcharacterflag,stationpressure,stationpressureflag,pressuretendency,pressuretendencyflag,pressurechange,pressurechangeflag,sealevelpressure,sealevelpressureflag,recordtype,recordtypeflag,hourlyprecip,hourlyprecipflag,altimeter,altimeterflag

.PHONY: all clean

TAR_FILES = $(shell python processors/gen_dates.py .tar.gz "1996/07" "2007/04")
ZIP_FILES = $(shell python processors/gen_dates.py .zip "2007/05")

clean:
	rm -Rf build/*
	rm -Rf raw/*

all: csv

fetch: $(TAR_FILES) $(ZIP_FILES)

csv: $(shell python processors/gen_dates.py hourly.csv)

sql: $(shell python processors/gen_dates.py .table)

%.tar.gz:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/$@ -O raw/$@
	@tar -O -x -f raw/$@ --wildcards --get *hourly.txt > build/$(subst tar,,$(basename $@))hourly.txt
	@touch raw/$@

%.zip:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/QCLCD$@ -O raw/$@
	@unzip -q -j raw/$@ *hourly.txt -d build
	@touch raw/$@

%hourly.txt: $(TAR_FILES) $(ZIP_FILES)
	@touch $@

%hourly.csv: %hourly.txt
	@cat build/$? | \
 	iconv -f ISO-8859-1 -t UTF-8 | python processors/make_real_nulls.py | \
 	python processors/combine_columns.py 1,2 | \
 	python processors/format_date.py "-1" "%Y%m%d-%H%M" "%Y-%m-%d %H:%M:%S" | \
 	python processors/catch_format_diffs.py | \
 	python processors/strip_whitespace.py > build/$(basename $@).csv \
 	echo "$@ done" \

%.table: %hourly.csv
	@psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) \
		-f processors/weather.sql
	@csvsql build/$(notdir $?) \
		--db "postgresql://$(PG_USER):@$(PG_HOST):$(PG_PORT)/$(PG_DB)" \
		--table src_weather --insert --no-create
	@touch build/$(notdir $?).table

