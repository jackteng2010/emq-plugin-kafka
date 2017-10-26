{application, emq_plugin_kafka, [
	{description, "EMQ Plugin Kafka"},
	{vsn, "2.3"},
	{modules, ['emq_cli_kafka','emq_plugin_kafka','emq_plugin_kafka_app','emq_plugin_kafka_sup']},
	{registered, [emq_plugin_kafka_sup]},
	{applications, [kernel,stdlib]},
	{mod, {emq_plugin_kafka_app, []}}
]}.