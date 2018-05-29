# flink-syslog-analysis

## Prerequisites: 

* [install elasticsearch version 5.x](https://www.elastic.co/guide/en/elasticsearch/reference/5.6/deb.html)
* [install grafana](http://docs.grafana.org/installation/debian/)
* run an AEM 6.3 author (locally or VM)
* install acs-aem-commons-content-3.14.x-min.zip (min is important, otherwise you need a twitter bundle(?!))

## Configuration:

* Create a new Syslog Appender for log.request as OSGi config under `/system/console/configMgr` (search for Syslog, cp. [Syslog Appender](https://adobe-consulting-services.github.io/acs-aem-commons/features/syslog-appender/index.html))
* Parameters:
  * host: e.g. 127.0.0.1 (the host you run the flink job on, in this case your IDE -> localhost)
  * Logger Names: log.request (ROOT==all)
  * Port: e.g. 5514 (standard syslog is 514)
  * Suffix Pattern: %msg (remove the rest especially the %n, we just care for the message)
* -> Save
* upload the index template via: curl -XPUT "http://<es-host>:<es-http-port>/_template/request_stats" -d @template_request_stats.json

## Run: 

* Import the project in your IDE
* run `mvn clean package`
* IntelliJ: 
  * File -> New -> Project from existing sources
  * Import project from external model -> Maven
  * Next -> Import maven projects automatically
  * Next (3x)
  * Finish
  
* Eclipse:
  * Works too
  
* Required program parameters:
  * --eh Elasticsearch host (name or IP)  
  * --ep Elasticsearch port (transport port!!!, standard: 9300)
  * --ec Elasticsearch clustername 
  * --sh Syslog host (name or IP, here 127.0.0.1) 
  * --sp Syslog port 
  * --in Index Name Prefix; will be created; needs to contain *-requeststats-*  
  * --jn Flink Job Name 

* Run RequestLogAnalysis.main()