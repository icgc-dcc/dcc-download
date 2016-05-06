# ICGC DCC - Data Download Server

Entry point to the download server.

## Building

`$ mvn -am -pl dcc-download-server package`


## Running

To run (SSL enabled by default):

```bash
$ java -cp src/main/conf/application.yml -jar target/dcc-download-server-<version>.jar 
...
<app starts and listens on port 8443>
```

