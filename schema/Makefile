#!/usr/bin/make
.SECONDEXPANSION:

BIN_DIR=$(DESTDIR)/usr/bin
MINERVA_SHARE_DIR=$(DESTDIR)/usr/share/minerva

SCRIPTS=$(addprefix $(BIN_DIR)/,init-minerva-db)
SQL_SCRIPTS=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard *.sql))
SQL_SCRIPTS_PUBLIC=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard public/*.sql))
SQL_SCRIPTS_DIRECTORY=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard directory/*.sql))
SQL_SCRIPTS_SYSTEM=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard system/*.sql))
SQL_SCRIPTS_RELATION=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard relation/*.sql))
SQL_SCRIPTS_DIMENSION=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard dimension/*.sql))
SQL_SCRIPTS_STORAGE_TREND=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard storage/trend/*.sql))
SQL_SCRIPTS_STORAGE_ATTRIBUTE=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard storage/attribute/*.sql))
SQL_SCRIPTS_STORAGE_GEOSPATIAL=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard storage/geospatial/*))
SQL_SCRIPTS_STORAGE_NOTIFICATION=$(addprefix $(MINERVA_SHARE_DIR)/,$(wildcard storage/notification/*.sql))

DIRS=\
	 $(BIN_DIR) \
	 $(MINERVA_SHARE_DIR) \
	 $(MINERVA_SHARE_DIR)/public \
	 $(MINERVA_SHARE_DIR)/directory \
	 $(MINERVA_SHARE_DIR)/system \
	 $(MINERVA_SHARE_DIR)/relation \
	 $(MINERVA_SHARE_DIR)/dimension \
	 $(MINERVA_SHARE_DIR)/storage/trend \
	 $(MINERVA_SHARE_DIR)/storage/attribute \
	 $(MINERVA_SHARE_DIR)/storage/geospatial \
	 $(MINERVA_SHARE_DIR)/storage/notification \
	 $(MINERVA_SHARE_DIR)/extensions

all:

clean:

install:\
	$(DIRS) \
	$(SQL_SCRIPTS) \
	$(SQL_SCRIPTS_PUBLIC) \
	$(SQL_SCRIPTS_DIRECTORY) \
	$(SQL_SCRIPTS_SYSTEM) \
	$(SQL_SCRIPTS_RELATION) \
	$(SQL_SCRIPTS_DIMENSION) \
	$(SQL_SCRIPTS_STORAGE_TREND) \
	$(SQL_SCRIPTS_STORAGE_ATTRIBUTE) \
	$(SQL_SCRIPTS_STORAGE_GEOSPATIAL) \
	$(SQL_SCRIPTS_STORAGE_NOTIFICATION) \
	$(SCRIPTS) \
	$(CONF_FILES)


$(DIRS):
	mkdir -p $@

$(SCRIPTS): $$(@F)
	install -m 0755 $(@F) $(@)

$(CONF_FILES): $$(@F)
	install -m 0600 $(@F) $(@)

$(SQL_SCRIPTS): $$(@F) | $(MINERVA_SHARE_DIR)
	install -m 0644 $(@F) $(@)

$(SQL_SCRIPTS_PUBLIC): public/$$(@F) | $(MINERVA_SHARE_DIR)/public
	install -m 0644 public/$(@F) $(@)

$(SQL_SCRIPTS_DIRECTORY): directory/$$(@F) | $(MINERVA_SHARE_DIR)/directory
	install -m 0644 directory/$(@F) $(@)

$(SQL_SCRIPTS_SYSTEM): system/$$(@F) | $(MINERVA_SHARE_DIR)/system
	install -m 0644 system/$(@F) $(@)

$(SQL_SCRIPTS_RELATION): relation/$$(@F) | $(MINERVA_SHARE_DIR)/relation
	install -m 0644 relation/$(@F) $(@)

$(SQL_SCRIPTS_DIMENSION): dimension/$$(@F) | $(MINERVA_SHARE_DIR)/dimension
	install -m 0644 dimension/$(@F) $(@)

$(SQL_SCRIPTS_STORAGE_TREND): storage/trend/$$(@F) | $(MINERVA_SHARE_DIR)/storage/trend
	install -m 0644 storage/trend/$(@F) $(@)

$(SQL_SCRIPTS_STORAGE_ATTRIBUTE): storage/attribute/$$(@F) | $(MINERVA_SHARE_DIR)/storage/attribute
	install -m 0644 storage/attribute/$(@F) $(@)

$(SQL_SCRIPTS_STORAGE_GEOSPATIAL): storage/geospatial/$$(@F) | $(MINERVA_SHARE_DIR)/storage/geospatial
	install -m 0644 storage/geospatial/$(@F) $(@)

$(SQL_SCRIPTS_STORAGE_NOTIFICATION): storage/notification/$$(@F) | $(MINERVA_SHARE_DIR)/storage/notification
	install -m 0644 storage/notification/$(@F) $(@)
