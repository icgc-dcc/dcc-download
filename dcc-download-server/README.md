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

## Generation of certificates
Follow [instructions](https://github.com/veeti/manuale) to get free certificates. If you have any problems with Python installation consult your Python guru.
You will cet back 4 files:

```
your_domain.chain.crt
your_domain.crt
your_domain.intermediate.crt
your_domain.pem
```
where `your_domain.crt` is a public key and `your_domain.pem` is a private key.

Next, generate PCK12 keystore and set some password (assuming it's 'password')

```
openssl pkcs12 -export -in your_domain.crt -inkey your_domain.pem -out identity.p12 -name "tomcat"
```
Next, convert to JKS

```
keytool -importkeystore -deststorepass password -destkeypass password -destkeystore letsencrypt.jks -srckeystore identity.p12 -srcstoretype PKCS12 -srcstorepass password -alias tomcat
```

