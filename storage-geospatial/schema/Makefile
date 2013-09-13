#!/usr/bin/make

SCRIPTS_DIR = $(DESTDIR)/usr/share/minerva/storage-plugins/geospatial

all:

clean:

install:
	install -d $(SCRIPTS_DIR)
	install -m 0755 prerequisite $(SCRIPTS_DIR)/
	install -m 0644 01_metadata.sql $(SCRIPTS_DIR)/
	install -m 0644 02_functions.sql $(SCRIPTS_DIR)/

