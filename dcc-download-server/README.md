# ICGC DCC - Data Download Server

Entry point to the download server.

## Building

```shell
$ mvn -am -pl dcc-download-server package
```


## Running

To run (SSL enabled by default):

```shell
$ java -cp src/main/conf/application.yml -jar target/dcc-download-server-<version>.jar 
...
<app starts and listens on port 8443>
```

## Security
To enable security (communication over SSL and BASIC authentication of the application endpoint) run application with profile `secure`, e.g. 

```shell
$ java -jar <jar_file> --spring.profiles.active=production,secure
```

**N.B.** Note, it's not enough to include the `secure` profile into another one in the `application.yml`. The profile must be specified on the command like as mentioned above.
