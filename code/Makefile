MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
.SUFFIXES:
VPATH=raw:build

HOURLY_HEADER="wban_number,date,time,station_type,maintenance_indicator,\
sky_conditions,visibility,weather_type,dry_bulb_temp,dew_point_temp,\
wet_bulb_temp,relative_humidity,wind_speed_kt,wind_direction,\
wind_char_gusts_kt,val_for_wind_char,station_pressure,pressure_tendency,\
sea_level_pressure,record_type,precip_total"

.PHONY: all clean

clean:
	rm -Rf build/*
	rm -Rf raw/*

all: csv

fetch: $(shell python processors/gen_dates.py tar.gz)

csv: $(shell python processors/gen_dates.py hourly.csv)

%.tar.gz:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/$@ -O raw/$@
	@touch raw/$@

%hourly.csv: %.tar.gz
	@tar -O -x -f raw/$(notdir $?) --include *hourly.txt | \
	(echo $(HOURLY_HEADER) ; tail -n +2) | \
	iconv -f ISO-8859-1 -t UTF-8 > build/$(basename $@).csv
	@echo "$@ done"
