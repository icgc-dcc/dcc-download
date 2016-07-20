# ICGC DCC - Data Download Import

An utility which can load Elasticsearch index obtained from the [DCC Download](https://download.icgc.org/exports) server.

## Building

```shell
$ mvn -am -pl dcc-download-import package
```

## Import
To load an Elasticsearch index first download it from the DCC Download service.

```shell
$ wget https://download.icgc.org/exports/release<release_number>.tar
```

Secondly, run the utility to upload the index archive to an Elasticsearch cluster.

```shell
$ java -jar dcc-download-import-<version>.jar \
  -i </path/to/the/elasticsearch/archive> \
  -es <elasticsearch_url>
```

For example:

```shell
$ java -jar dcc-download-import-4.2.12.jar -i /tmp/release21.tar -es es://localhost:9300
```

### Custom logging
To override the default logging setting use `-Dlogback.configurationFile=logback.xml` configuration option.

E.g. `java -Dlogback.configurationFile=logback.xml -jar ...`
