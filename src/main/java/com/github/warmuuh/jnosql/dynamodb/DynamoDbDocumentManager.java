package com.github.warmuuh.jnosql.dynamodb;

import static com.github.warmuuh.jnosql.dynamodb.AttributeValueConverter.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

@RequiredArgsConstructor
public class DynamoDbDocumentManager implements DocumentManager {

  private final DynamoDbClient client;
  private final String tablePrefix;

  @Override
  public String name() {
    return "dynamodb";
  }

  @Override
  public DocumentEntity insert(DocumentEntity documentEntity) {
    return null;
  }

  @Override
  public DocumentEntity insert(DocumentEntity documentEntity, Duration duration) {
    return null;
  }

  @Override
  public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable) {
    return null;
  }

  @Override
  public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable, Duration duration) {
    return null;
  }

  @Override
  public DocumentEntity update(DocumentEntity documentEntity) {
    return null;
  }

  @Override
  public Iterable<DocumentEntity> update(Iterable<DocumentEntity> iterable) {
    return null;
  }

  @Override
  public void delete(DocumentDeleteQuery documentDeleteQuery) {

  }

  @Override
  public Stream<DocumentEntity> select(DocumentQuery qry) {

    String tableName = tablePrefix + qry.name();
    TableDescription table = client.describeTable(r -> r.tableName(tableName)).table();

    if (table == null) {
      throw new IllegalStateException("Table '" + tableName + "' not found");
    }

    List<String> keyAttributeNames = table.keySchema().stream().map(e -> e.attributeName()).toList();
    KeySchemaElement hashKey = table.keySchema().stream().filter(e -> e.keyType() == KeyType.HASH)
        .findAny().orElseThrow(() -> new IllegalStateException("No hash key found for table " + tableName));

    DocumentCondition condition = qry.condition()
        .orElseThrow(() -> new IllegalStateException("Cant scan all for dynamodb. needs where clause"));
    Map<String, Condition> conditions = QueryMapper.getKeyFromSelect(condition);
    Map<String, Condition> keyConditions = new HashMap<>();
    conditions.forEach((k, v) -> {
      if (keyAttributeNames.contains(k)) {
        keyConditions.put(k, v);
      }
    });
    keyConditions.forEach((k, v) -> conditions.remove(k));

    QueryResponse response = client.query(r -> r.tableName(tableName)
        .keyConditions(keyConditions)
        .queryFilter(conditions));

    if (!response.hasItems()) {
      return Stream.empty();
    }

    return response.items().stream().map(item -> {
      String objectHashKey = Objects.toString(toObject(item.get(hashKey.attributeName())));
      return DocumentEntity
          .of(objectHashKey,
              item.entrySet().stream().map(e -> {
                return Document.of(e.getKey(), Objects.toString(toObject(e.getValue())));
              }).toList());
    });
  }

  @Override
  public long count(String s) {
    return 0;
  }

  @Override
  public void close() {
    client.close();
  }
}
