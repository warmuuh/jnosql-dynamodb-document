package com.github.warmuuh.jnosql.dynamodb;

import com.github.warmuuh.jnosql.dynamodb.SelectStrategy.QueryStrategy;
import com.github.warmuuh.jnosql.dynamodb.SelectStrategy.ScanStrategy;
import lombok.RequiredArgsConstructor;
import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@RequiredArgsConstructor
public class DynamoDbDocumentManagerFactory implements DocumentManagerFactory {

  private final DynamoDbClient client;
  private final Settings settings;

  @Override
  public void close() {
    client.close();
  }

  @Override
  public DocumentManager apply(String database) {
    String tablePrefix = settings.get(DynamoDBDocumentProperties.PREFIX, String.class).orElse(database);
    String selectMode = settings.get(DynamoDBDocumentProperties.SELECT_MODE, String.class).orElse("query");
    SelectStrategy selectStrategy;
    if (selectMode.equalsIgnoreCase("query")) {
      selectStrategy = new QueryStrategy();
    } else if (selectMode.equalsIgnoreCase("scan")) {
      selectStrategy = new ScanStrategy();
    } else {
      throw new IllegalArgumentException(selectMode + " is not a valid selection mode");
    }
    return new DynamoDbDocumentManager(client, tablePrefix, selectStrategy);
  }
}
