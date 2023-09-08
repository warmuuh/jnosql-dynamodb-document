package com.github.warmuuh.jnosql.dynamodb;

import static com.github.warmuuh.jnosql.dynamodb.AttributeValueConverter.fromObject;
import static org.eclipse.jnosql.communication.driver.ValueUtil.convertToList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

@UtilityClass
public class DynamoDbConditionMapper {

  Map<String, Condition> mapCondition(DocumentCondition documentCondition) {

    Document document = documentCondition.document();
    org.eclipse.jnosql.communication.Condition condition = documentCondition.condition();

    Map<String, Condition> result = new HashMap<>();

    switch (condition) {
      case EQUALS ->
        result.put(document.name(), Condition.builder()
            .comparisonOperator(ComparisonOperator.EQ)
            .attributeValueList(fromObject(document.value().get()))
            .build());
      case GREATER_THAN ->
          result.put(document.name(), Condition.builder()
              .comparisonOperator(ComparisonOperator.GT)
              .attributeValueList(fromObject(document.value().get()))
              .build());
      case GREATER_EQUALS_THAN ->
        result.put(document.name(), Condition.builder()
          .comparisonOperator(ComparisonOperator.GE)
          .attributeValueList(fromObject(document.value().get()))
          .build());
      case LESSER_THAN ->
          result.put(document.name(), Condition.builder()
              .comparisonOperator(ComparisonOperator.LT)
              .attributeValueList(fromObject(document.value().get()))
              .build());
      case LESSER_EQUALS_THAN ->
        result.put(document.name(), Condition.builder()
            .comparisonOperator(ComparisonOperator.LE)
            .attributeValueList(fromObject(document.value().get()))
            .build());
      case IN ->
          result.put(document.name(), Condition.builder()
          .comparisonOperator(ComparisonOperator.IN)
          .attributeValueList(convertToList(document.value()).stream().map(AttributeValueConverter::fromObject).toList())
          .build());
      case AND ->
          document.get(new TypeReference<List<DocumentCondition>>() {})
              .stream().map(DynamoDbConditionMapper::mapCondition).forEach(result::putAll);
      case NOT ->
          result.put(document.name(), Condition.builder()
          .comparisonOperator(ComparisonOperator.NE)
          .attributeValueList(fromObject(document.value().get()))
          .build());
      //TODO this can be supported, i am just lazy
//      case BETWEEN -> {
//      }
      //TODO this can be supported a bit with "BEGINS_WITH" maybe
//      case LIKE -> {
//      }
      default ->
        throw new UnsupportedOperationException("The condition " + condition +
            " is not supported in dynamodb document driver");
    }

    return result;
  }

}
