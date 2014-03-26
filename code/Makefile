MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
.SUFFIXES:
VPATH=raw:build
PG_HOST=33.33.33.8
PG_USER=postgres
PG_DB=wopr
PG_PORT=5432

HOURLY_HEADER="wban_number,date_time,station_type,maintenance_indicator,\
sky_conditions,visibility,weather_type,dry_bulb_temp,dew_point_temp,\
wet_bulb_temp,relative_humidity,wind_speed_kt,wind_direction,\
wind_char_gusts_kt,val_for_wind_char,station_pressure,pressure_tendency,\
sea_level_pressure,record_type,precip_total"

.PHONY: all clean

TAR_FILES = $(shell python processors/gen_dates.py .tar.gz "1996/07" "1996/09")
ZIP_FILES = $(shell python processors/gen_dates.py .zip "2007/05" "2007/07")

clean:
	rm -Rf build/*
	rm -Rf raw/*

all: csv

fetch: $(shell python processors/gen_dates.py .tar.gz)

csv: $(shell python processors/gen_dates.py hourly.csv)

sql: $(shell python processors/gen_dates.py .table)

%.tar.gz:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/$@ -O raw/$@
	@tar -O -x -f raw/$@ --include *hourly.txt > build/$(basename $@)hourly.txt
	@touch raw/$@

%.zip:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/QCLCD$@ -O raw/$@
	@unzip -q -j $@ *hourly.txt -d build; \
	@touch raw/$@

%hourly.txt: $(TAR_FILES) $(ZIP_FILES)
	@touch $@

%hourly.csv: %hourly.txt
	@echo $?
#	tail -n +2 | \
#	iconv -f ISO-8859-1 -t UTF-8 | python processors/make_real_nulls.py | \
#	python processors/combine_columns.py 1,2 | \
#	python processors/format_date.py "-1" "%Y%m%d-%H%M" "%Y-%m-%d %H:%M:%S" | \
#	csvcut -c 1,22,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21 | \
#	(echo $(HOURLY_HEADER) ; tail -n +2 ) | \
#	python processors/strip_whitespace.py > build/$(basename $@).csv \
#	echo "$@ done" \

%.table: %hourly.csv
	@psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) \
		-f processors/weather.sql
	@csvsql build/$(notdir $?) \
		--db "postgresql://$(PG_USER):@$(PG_HOST):$(PG_PORT)/$(PG_DB)" \
		--table src_weather --insert --no-create
	@touch build/$(notdir $?).table

