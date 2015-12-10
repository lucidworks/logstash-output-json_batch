# Logstash Plugin

This plugin is a heavily butchered and modified version of the standard logstash http output plugin. That plugin is available [here](https://github.com/logstash-plugins/logstash-output-http). 

It has been modified to only support JSON, but also supports batching of messages into JSON lists.

# Usage

Please note that the name of the plugin when used is `json_batch`, since it only supports json in its current form. If further output formats are added in the future, this might change back to http_batch.

    output {
      json_batch {
        headers => ["Authorization", "Basic YWRtaW46cGFzc3dvcmQxMjM="]
        url => "http://your.site/your/json/update/endpoint"
      }
    }
    
Default batch size is 50, with a wait of at most 5 seconds per send. These can be tweaked with the parameters `flush_size` and `idle_flush_time` respectively.