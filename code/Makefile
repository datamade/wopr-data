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

.PHONY: all clean

TAR_FILES = $(shell python processors/gen_dates.py .tar.gz "1996/07" "2007/04")
ZIP_FILES = $(shell python processors/gen_dates.py .zip "2007/05")

clean:
	rm -Rf build/*
	rm -Rf raw/*

all: csv

fetch: $(ZIP_FILES)

csv: $(shell python processors/gen_dates.py hourly.csv "2007/05")

join: $(shell python processors/gen_dates.py hourly.joined.csv "2007/05")

il: $(shell python processors/gen_dates.py hourly.il "2007/05")

sql: $(shell python processors/gen_dates.py .table "2007/05")

%.tar.gz:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/$@ -O raw/$@
	@tar -O -x -f raw/$@ --wildcards --get '*hourly.txt' > build/$(subst tar,,$(basename $@))hourly.txt
	@touch raw/$@

%.zip:
	@wget http://cdo.ncdc.noaa.gov/qclcd_ascii/QCLCD$@ -O raw/$@
	@unzip -q -j raw/$@ '*hourly.txt' -d build
	@touch raw/$@

%hourly.il: %hourly.txt
	for f in $?; do \
		for wban in $(shell cat raw/station_ids.txt); do \
			cat $$f | grep ^$$wban >> build/$@; \
		done; \
	done;

%hourly.cleaned.csv: %hourly.il
	@echo "Cleaning $@..."
	@python processors/open_file.py build/$(notdir $?) | \
 	iconv -f ISO-8859-1 -t UTF-8 | \
 	tr -d '"' | \
 	python processors/strip_whitespace.py | \
 	python processors/nullify_fields.py M | \
 	python processors/validate_nulls.py 1,2,10 | \
 	python processors/make_real_nulls.py > build/$(basename $@).csv

%hourly.csv: %hourly.cleaned.csv
	@echo "Building $@..."
	@python processors/open_file.py build/$(notdir $?) | \
		python processors/combine_columns.py 1,2 | \
		python processors/make_floats.py 8,10,12,14,16,18,20,28,34,40 | \
		python processors/format_date.py "-1" "%Y%m%d-%H%M" "%Y-%m-%d %H:%M:%S" | \
		python processors/catch_format_diffs.py > build/$(basename $@).csv
	@echo "$@ done" \

%hourly.joined.csv: %hourly.csv
	@csvjoin -c 1,2 build/$(notdir $?) raw/stations.csv | \
		csvcut -c 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,51,52 | \
		python processors/norm_lon_lat.py "-1,-2" > build/$@

%hourly.table: %hourly.joined.csv
	@psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) \
		-f processors/weather.sql
	@csvsql build/$(notdir $?) \
		--db "postgresql://$(PG_USER):@$(PG_HOST):$(PG_PORT)/$(PG_DB)" \
		--table src_weather --insert --no-create
	@psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) \
		-f processors/add_geom.sql
	@touch build/$(notdir $?).table

