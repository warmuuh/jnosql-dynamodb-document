package com.github.warmuuh.jnosql.dynamodb;

import lombok.RequiredArgsConstructor;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

@RequiredArgsConstructor
public class DynamoDbDocumentManagerFactory implements DocumentManagerFactory {

  private final DynamoDbClient client;

  @Override
  public void close() {
    client.close();
  }

  @Override
  public DocumentManager apply(String tablePrefix) {
    return new DynamoDbDocumentManager(client, tablePrefix);
  }
}
