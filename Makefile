PROJECT = emq_plugin_kafka
PROJECT_DESCRIPTION = EMQ Plugin Kafka
PROJECT_VERSION = 2.3

BUILD_DEPS = emqttd cuttlefish kafka
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish
dep_kafka = git https://github.com/helpshift/ekaf

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

NO_AUTOPATCH = cuttlefish

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emq_plugin_kafka.conf -i priv/emq_plugin_kafka.schema -d data
