package com.github.warmuuh.jnosql.dynamodb;

import static com.github.warmuuh.jnosql.dynamodb.AttributeValueConverter.*;

import com.github.warmuuh.jnosql.dynamodb.util.WithIndex;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest.Builder;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
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
  public void delete(DocumentDeleteQuery qry) {
    DocumentCondition condition = qry.condition()
        .orElseThrow(() -> new IllegalStateException("Cant scan all for dynamodb. needs where clause"));

    TableDescription table = getTableDescription(qry.name());
    ConditionSets result = getConditionSets(table, condition);

    if (!result.filterConditions.isEmpty() || result.keyConditions.size() != table.keySchema().size()) {
      throw new IllegalArgumentException("Only single items can be deleted at a time. you have to provide full key schema attributes");
    }

    Builder qryBuilder = DeleteItemRequest.builder()
        .tableName(table.tableName())
        .keyConditions(result.keyConditions())
        .queryFilter(result.filterConditions());

    if (qry.documents().size() > 0) {
      throw new IllegalArgumentException("Only full documents can be deleted in dynamodb");
    }

    QueryResponse response = client.del(qryBuilder.build());

    if (!response.hasItems()) {
      return Stream.empty();
    }

  }

  @Override
  public Stream<DocumentEntity> select(DocumentQuery qry) {
    DocumentCondition condition = qry.condition()
        .orElseThrow(() -> new IllegalStateException("Cant scan all for dynamodb. needs where clause"));

    TableDescription table = getTableDescription(qry.name());
    ConditionSets result = getConditionSets(table, condition);

    Builder qryBuilder = QueryRequest.builder()
        .tableName(table.tableName())
        .keyConditions(result.keyConditions())
        .queryFilter(result.filterConditions());

    if (qry.limit() > 0) {
      qryBuilder.limit((int)qry.limit());
    }
    
    if (qry.documents().size() > 0) {
      qryBuilder.attributesToGet(qry.documents());
    }

    QueryResponse response = client.query(qryBuilder.build());

    if (!response.hasItems()) {
      return Stream.empty();
    }

    List<Map<String, AttributeValue>> items = response.items();
    return items.stream()
        .map(WithIndex.indexed())
        .map(itemWithIndex -> {
          var item = itemWithIndex.value();
          String documentName = Integer.toString(itemWithIndex.index());
          return mapToDocumentEntity(item, documentName);
        });
  }

  private static DocumentEntity mapToDocumentEntity(Map<String, AttributeValue> item, String documentName) {
    return DocumentEntity
        .of(documentName,
            item.entrySet().stream().map(e -> {
              return Document.of(e.getKey(), Objects.toString(toObject(e.getValue())));
            }).toList());
  }

  private TableDescription getTableDescription(String selectedTable) {
    String tableName = tablePrefix + selectedTable;
    TableDescription table = client.describeTable(r -> r.tableName(tableName)).table();

    if (table == null) {
      throw new IllegalStateException("Table '" + tableName + "' not found");
    }
    return table;
  }

  private static ConditionSets getConditionSets(TableDescription table, DocumentCondition condition) {
    Map<String, Condition> conditions = DynamoDbConditionMapper.mapCondition(condition);

    List<String> keyAttributeNames = table.keySchema().stream().map(e -> e.attributeName()).toList();
    Map<String, Condition> keyConditions = new HashMap<>();
    conditions.forEach((k, v) -> {
      if (keyAttributeNames.contains(k)) {
        keyConditions.put(k, v);
      }
    });
    keyConditions.forEach((k, v) -> conditions.remove(k));
    ConditionSets result = new ConditionSets(conditions, keyConditions);
    return result;
  }

  private record ConditionSets(Map<String, Condition> filterConditions, Map<String, Condition> keyConditions) {

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
