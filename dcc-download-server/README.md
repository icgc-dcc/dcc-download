# ICGC DCC - Auth Server

This project is a minimal but functional [OAuth2](http://tools.ietf.org/html/rfc6749) [Authorization Server](http://tools.ietf.org/html/rfc6749#section-1.1)
(token issuer) and [Resource Server](http://tools.ietf.org/html/rfc6749#section-1.1) (protected API) for the [PCAWG](http://pancancer.info/) project. 
It is a [microservice](http://martinfowler.com/articles/microservices.html) built on [Spring Boot](https://github.com/spring-projects/spring-boot) which runs 
an embedded Tomcat instance.

## Building

You need Java (1.7 or better) and Maven (3.0.5 or better):

`$ mvn package`


## Running

To run (SSL enabled by default):

```bash
$ java-cp src/main/conf/application.yml -jar target/dcc-auth-server-<version>.jar 
...
<app starts and listens on port 8443>
```

To run in eclipse, configure the following program arguments in the `ServerMain` launcher: 

```bash
--spring.config.location=src/main/conf/application.yml
--logging.config=src/main/conf/logback.xml
--logging.path=target/logs
--server.tomcat.basedir=target
```

## Configuration

All configuration may be found in:

[```src/main/conf/application.yml```](src/main/conf/application.yml)

## Examples

### Notes

From the command, if you use SSL with a self-signed certificate at `src/main/resources/keystore.p12` (the default), you must use `curl -k`.


### Token Creation

Create `aws.upload` and `aws.download` tokens: 

```bash
curl -k https://localhost:8443/oauth/token  -H "Accept: application/json" -dgrant_type=password -dusername=<username> -dscope="aws.upload aws.download" -u <client_id>:<password>
```

Where `client_id` and `password` are configured in [```src/main/conf/application.yml```](src/main/conf/application.yml)

Create a token with description:

```bash
curl -k https://localhost:8443/oauth/token  -H "Accept: application/json" -dgrant_type=password -dusername=<username> -dscope="aws.upload" -ddesc="Token description" -u <client_id>:<password>
```

### User Tokens

Display all of a user's tokens:

```bash
$ curl -k https://localhost:8443/users/<username>/tokens -u <client_id>:<password>
```

### Revoke token

Revoke user's access token:

```bash
$ curl -k -XDELETE https://localhost:8443/tokens/<token> -u <client_id>:<password>
```

### List token digests

To list all tokens (deleted and active):

```bash
$ curl -k https://localhost:8443/token-digests -u <client_id>:<password>
```

### Get token info

Get access token information. This endpoint is used the Resource Servers.

```bash
$ curl -k -XPOST https://localhost:8443/oauth/check_token -d token=<token> -u <client_id>:<password>
```

### User Scopes

List scopes which a user is permitted to use for token generation.

```bash
$ curl -k https://localhost:8443/users/<username>/scopes -u <client_id>:<password>
```

### Define available scopes

Each OpenID client is configured with a list of scopes it is allowed to generate. The client configuration is stored in the database in table `oauth_client_details`.

E.g. to set new scopes execute the following `SQL` command:

```sql
UPDATE oauth_client_details SET scope = 'collab.upload,collab.download,aws.download,aws.upload,tra.upload,tra.download' WHERE client_id = 'mgmt';
```

## Admin

Admin operations are bound to the `localhost` interface on port 8444.

### List tokens

List all access tokens for all clients and users:

```bash
$ curl -k https://localhost:8444/admin/tokens_list -u admin:secret
```

where `<application_password>` can be found in the application's `STDOUT` when it starts.

### User scope management

Allows to manage scopes which user is permitted to use for token generation.

#### Put new or override existing scope

```bash
$ curl -k -XPUT https://localhost:8444/admin/scopes/<username> -u admin:secret -d"collab.upload"
```

#### Delete user scope

```bash
$ curl -k -XDELETE https://localhost:8444/admin/scopes/<userName> -u admin:secret
```

### Database Backup

To backup the H2 database use the `/admin/backup` endpoint:

```bash
$ curl -k -H "Accept: application/json" -X GET https://localhost:8444/admin/backup
```

## Tips

Convenience one-liner that saves a successfully acquired access token into a shell variable:

```bash
TOKEN=$(curl -s -k -H "Accept: application/json" "https://localhost:8443/oauth/token" -d "grant_type=password" -d ... | sed -e 's/^.*"access_token":"\([^"]*\)".*$/\1/')
```
