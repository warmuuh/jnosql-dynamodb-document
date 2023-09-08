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

This API provides the ```DynamoDBConfigurations``` class to programmatically establish the credentials.
Please note that you can establish properties using the [MicroProfile Config](https://microprofile.io/microprofile-config/) specification.

| Configuration property         | Description                                                                      |
|--------------------------------|----------------------------------------------------------------------------------|
| `jnosql.dynamodb.endpoint`     | DynamoDB’s URL                                                                   |
| `jnosql.dynamodb.region`       | Configure the region with which the application should communicate.              |
| `jnosql.dynamodb.profile`      | Define the name of the profile that should be used by this credentials provider. |
| `jnosql.dynamodb.awsaccesskey` | The AWS access key, used to identify the user interacting with AWS.              |
| `jnosql.dynamodb.secretaccess` | The AWS secret access key, used to authenticate the user interacting with AWS.   |

## Example

This is an example using DynamoDB's Key-Value API with MicroProfile Config.

[source,properties]
----
jnosql.document.provider: com.github.warmuuh.jnosql.dynamodb.DynamoDBDocumentConfiguration
jnosql.dynamodb.region: eu-central-1
jnosql.dynamodb.profile: my_profile
----
