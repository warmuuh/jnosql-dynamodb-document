package com.github.warmuuh.jnosql.dynamodb;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBDocumentConfiguration extends DynamoDBConfiguration implements DocumentConfiguration {
  @Override
  public DocumentManagerFactory apply(Settings settings) {
    DynamoDbClient dynamoDB = getDynamoDB(settings);
    return new DynamoDbDocumentManagerFactory(dynamoDB);
  }

}
