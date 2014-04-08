MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
.SUFFIXES:
VPATH=raw:build
PG_HOST=localhost
PG_USER=postgres
PG_DB=woprtestdb
PG_PORT=5432

