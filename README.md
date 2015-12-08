# Logstash Plugin

This plugin is a heavily butchered and modified version of the standard logstash http output plugin. That plugin is available [here](https://github.com/logstash-plugins/logstash-output-solr_http). 

It has been modified to only support JSON, but to also supports batching of messages into JSON lists.