# JNoSql Dynamodb Document driver

Support for *Document* NoSql Api (see [JNoSql](https://github.com/eclipse/jnosql))

## How To Install

You can use either the Maven or Gradle dependencies:

```xml

<dependency>
  <groupId>com.github.warmuuh</groupId>
  <artifactId>jnosql-dynamodb-document</artifactId>
  <version>1.0.1-SNAPSHOT</version>
</dependency>
```

if you use sso for AWS authentication, you need to include sso as well:

```xml

<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>sso</artifactId>
  <version>2.20.98</version>
</dependency>
```

## Configuration

This API provides the ```DynamoDBDocumentConfiguration``` class to programmatically establish the credentials.
Please note that you can establish properties using the [MicroProfile Config](https://microprofile.io/microprofile-config/) specification.

| Configuration property         | Description                                                                      | Required            |
|--------------------------------|----------------------------------------------------------------------------------|---------------------|
| `jnosql.dynamodb.endpoint`     | DynamoDB’s URL                                                                   | no                  |
| `jnosql.dynamodb.region`       | Configure the region with which the application should communicate.              | yes                 |
| `jnosql.dynamodb.profile`      | Define the name of the profile that should be used by this credentials provider. | no                  |
| `jnosql.dynamodb.awsaccesskey` | The AWS access key, used to identify the user interacting with AWS.              | no (if sso)         |
| `jnosql.dynamodb.secretaccess` | The AWS secret access key, used to authenticate the user interacting with AWS.   | no (if sso)         |
| `jnosql.dynamodb.prefix`       | Table prefix to be used.                                                         | no                  |
| `jnosql.dynamodb.selectmode`   | Selection mode, can be `query` or `scan`.                                        | no (default: query) |

## Example

This is an example using DynamoDB's Document API with MicroProfile Config.

```properties
jnosql.document.provider: com.github.warmuuh.jnosql.dynamodb.DynamoDBDocumentConfiguration
jnosql.dynamodb.region: eu-central-1
jnosql.dynamodb.profile: my_profile
```
