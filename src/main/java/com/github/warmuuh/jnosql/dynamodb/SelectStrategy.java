package com.github.warmuuh.jnosql.dynamodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest.Builder;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public abstract class SelectStrategy {

  public abstract Stream<Map<String, AttributeValue>> execute(DynamoDbClient client, TableDescription table, DocumentQuery qry);


    public static class QueryStrategy extends SelectStrategy {

    @Override
    public Stream<Map<String, AttributeValue>> execute(DynamoDbClient client, TableDescription table, DocumentQuery qry) {
      Builder qryBuilder = QueryRequest.builder()
          .tableName(table.tableName());

      qry.condition().ifPresent(condition -> {
        ConditionSets result = getConditionSets(table, condition);
        qryBuilder
            .keyConditions(result.keyConditions())
            .queryFilter(result.filterConditions());
      });

      if (qry.limit() > 0) {
        qryBuilder.limit((int) qry.limit());
      }

      if (qry.documents().size() > 0) {
        qryBuilder.attributesToGet(qry.documents());
      }

      QueryResponse response = client.query(qryBuilder.build());

      if (!response.hasItems()) {
        return Stream.empty();
      }

      return response.items().stream();
    }

  }

  public static class ScanStrategy extends SelectStrategy {

    @Override
    public Stream<Map<String, AttributeValue>> execute(DynamoDbClient client, TableDescription table, DocumentQuery qry) {
      ScanRequest.Builder scanRequest = ScanRequest.builder()
          .tableName(table.tableName());

      qry.condition().ifPresent(condition -> {
        ConditionSets result = getConditionSets(table, condition);
        Map<String, Condition> allConditions = new HashMap<>();
        allConditions.putAll(result.keyConditions());
        allConditions.putAll(result.filterConditions());
        scanRequest
            .scanFilter(allConditions);
      });

      if (qry.limit() > 0) {
        scanRequest.limit((int) qry.limit());
      }

      if (qry.documents().size() > 0) {
        scanRequest.attributesToGet(qry.documents());
      }

      ScanResponse response = client.scan(scanRequest.build());

      if (!response.hasItems()) {
        return Stream.empty();
      }

      return response.items().stream();
    }

  }

  protected record ConditionSets(Map<String, Condition> filterConditions, Map<String, Condition> keyConditions) {

  }

  protected static ConditionSets getConditionSets(TableDescription table, DocumentCondition condition) {
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
}
