ECONDEXPANSION:

BIN_DIR=$(DESTDIR)/usr/bin
MINERVA_INSTANCE_DIR=$(DESTDIR)/usr/share/minerva/instance/{{instance_name}}

TREND_STORES=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard trend/*.yaml))
ATTRIBUTE_STORES=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard attribute/*.yaml))
ATTRIBUTE_MATERIALIZATIONS=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard attribute/materialization/*.yaml))
NOTIFICATION_STORES=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard notification/*.yaml))
VIRTUAL_ENTITIES=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard virtual-entity/*.sql))
RELATIONS=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard relation/*.yaml))
MATERIALIZATIONS=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard materialization/*.sql))
TRIGGERS=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard trigger/*.yaml))
CUSTOM_SQL_FILES=$(addprefix $(MINERVA_INSTANCE_DIR)/,$(wildcard custom/*.sql))

DIRS=\
	 $(MINERVA_INSTANCE_DIR) \
	 $(MINERVA_INSTANCE_DIR)/attribute \
	 $(MINERVA_INSTANCE_DIR)/attribute/materialization \
	 $(MINERVA_INSTANCE_DIR)/trend \
	 $(MINERVA_INSTANCE_DIR)/notification \
	 $(MINERVA_INSTANCE_DIR)/relation \
	 $(MINERVA_INSTANCE_DIR)/virtual-entity \
	 $(MINERVA_INSTANCE_DIR)/materialization \
	 $(MINERVA_INSTANCE_DIR)/trigger \
	 $(MINERVA_INSTANCE_DIR)/custom

all:

clean:

install:\
	$(DIRS) \
	$(ATTRIBUTE_STORES) \
	$(ATTRIBUTE_MATERIALIZATIONS) \
	$(TREND_STORES) \
	$(NOTIFICATION_STORES) \
	$(RELATIONS) \
	$(VIRTUAL_ENTITIES) \
	$(MATERIALIZATIONS) \
	$(TRIGGERS) \
	$(CUSTOM_SQL_FILES)


$(DIRS):
	mkdir -p $@

$(TREND_STORES): trend/$(@F) | $(MINERVA_INSTANCE_DIR)/trend
	install -m 0644 "trend/$(@F)" "$(@)"

$(ATTRIBUTE_STORES): attribute/$(@F) | $(MINERVA_INSTANCE_DIR)/attribute
	install -m 0644 "attribute/$(@F)" "$(@)"

$(ATTRIBUTE_MATERIALIZATIONS): attribute/materialization/$(@F) | $(MINERVA_INSTANCE_DIR)/attribute/materialization
	install -m 0644 "attribute/materialization/$(@F)" "$(@)"

$(NOTIFICATION_STORES): notification/$(@F) | $(MINERVA_INSTANCE_DIR)/notification
	install -m 0644 "notification/$(@F)" "$(@)"

$(RELATIONS): relation/$(@F) | $(MINERVA_INSTANCE_DIR)/relation
	install -m 0644 "relation/$(@F)" "$(@)"

$(VIRTUAL_ENTITIES): virtual-entity/$(@F) | $(MINERVA_INSTANCE_DIR)/virtual-entity
	install -m 0644 "virtual-entity/$(@F)" "$(@)"

$(MATERIALIZATIONS): materialization/$(@F) | $(MINERVA_INSTANCE_DIR)/materialization
	install -m 0644 "materialization/$(@F)" "$(@)"

$(TRIGGERS): trigger/$(@F) | $(MINERVA_INSTANCE_DIR)/trigger
	install -m 0644 "trigger/$(@F)" "$(@)"

$(CUSTOM_SQL_FILES): custom/$(@F) | $(MINERVA_INSTANCE_DIR)/custom
	install -m 0644 "custom/$(@F)" "$(@)"
