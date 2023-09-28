package com.github.warmuuh.jnosql.dynamodb;

import static com.github.warmuuh.jnosql.dynamodb.AttributeValueConverter.toObject;

import com.github.warmuuh.jnosql.dynamodb.util.WithIndex;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

@RequiredArgsConstructor
public class DynamoDbDocumentManager implements DocumentManager {

  private final DynamoDbClient client;
  private final String tablePrefix;
  private final SelectStrategy selectStrategy;

  @Override
  public String name() {
    return "dynamodb";
  }

  @Override
  public DocumentEntity insert(DocumentEntity doc) {
    String tableName = tablePrefix + doc.name();
    Map<String, AttributeValue> attributes = doc.toMap().entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey(),
        e -> AttributeValueConverter.fromObject(e.getValue())
    ));

    client.putItem(r -> r.tableName(tableName).item(attributes));
    return doc;
  }

  @Override
  public DocumentEntity insert(DocumentEntity doc, Duration duration) {
    String tableName = tablePrefix + doc.name();
    DescribeTimeToLiveResponse ttlDescResponse = client.describeTimeToLive(r -> r.tableName(tableName));
    String ttlAttribute = ttlDescResponse.timeToLiveDescription().attributeName();
    doc.add(ttlAttribute, Value.of(Instant.now().plus(duration).toEpochMilli() / 1000L));
    return insert(doc);
  }

  @Override
  public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable) {
    //TODO batch requests
    return StreamSupport.stream(iterable.spliterator(), false)
        .map(this::insert)
        .toList();
  }

  @Override
  public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable, Duration duration) {
    //TODO batch requests
    return StreamSupport.stream(iterable.spliterator(), false)
        .map(doc -> insert(doc, duration))
        .toList();
  }

  @Override
  public DocumentEntity update(DocumentEntity doc) {
    String tableName = tablePrefix + doc.name();

    Map<String, AttributeValue> attributes = doc.toMap().entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey(),
        e -> AttributeValueConverter.fromObject(e.getValue())
    ));

    TableDescription table = getTableDescription(doc.name());
    List<String> keyAttributeNames = table.keySchema().stream().map(e -> e.attributeName()).toList();
    Map<String, AttributeValue> key = new HashMap<>();
    Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
    attributes.forEach((k, v) -> {
      if (keyAttributeNames.contains(k)) {
        key.put(k, v);
      } else {
        attributeUpdates.put(k, AttributeValueUpdate.builder().value(v).build());
      }
    });

    client.updateItem(r -> r.tableName(tableName).key(key).attributeUpdates(attributeUpdates));
    return doc;
  }

  @Override
  public Iterable<DocumentEntity> update(Iterable<DocumentEntity> iterable) {
    //TODO batch requests
    return StreamSupport.stream(iterable.spliterator(), false)
        .map(this::update)
        .toList();
  }

  @Override
  public void delete(DocumentDeleteQuery qry) {
    DocumentCondition condition = qry.condition()
        .orElseThrow(() -> new IllegalStateException("Cant scan all for dynamodb. needs where clause"));

    TableDescription table = getTableDescription(qry.name());
    Map<String, AttributeValue> key = getKey(table, condition);

    if (qry.documents().size() > 0) {
      throw new IllegalArgumentException("Only full documents can be deleted in dynamodb");
    }

    client.deleteItem(r -> r.tableName(table.tableName()).key(key));
  }

  @Override
  public Stream<DocumentEntity> select(DocumentQuery qry) {
    TableDescription table = getTableDescription(qry.name());
    Stream<Map<String, AttributeValue>> items =selectStrategy.execute(client, table, qry);

    return items
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



  private static Map<String, AttributeValue> getKey(TableDescription table, DocumentCondition condition) {
    Map<String, Condition> conditions = DynamoDbConditionMapper.mapCondition(condition);

    List<String> keyAttributeNames = table.keySchema().stream().map(e -> e.attributeName()).toList();
    boolean coveredQuery = keyAttributeNames.stream().allMatch(conditions::containsKey) && keyAttributeNames.size() == conditions.size();

    Map<String, AttributeValue> key = conditions.entrySet().stream()
        .filter(e -> e.getValue().comparisonOperator() == ComparisonOperator.EQ)
        .collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().attributeValueList().get(0)
        ));
    if (!coveredQuery || key.size() != keyAttributeNames.size()) {
      throw new IllegalStateException("Query does not cover key: " + keyAttributeNames + ". only '=' operator allowed");
    }

    return key;
  }



  @Override
  public long count(String tableName) {
    ScanResponse scan = client.scan(r -> r.tableName(tableName).select(Select.COUNT));
    return scan.count();
  }

  @Override
  public void close() {
    client.close();
  }
}
