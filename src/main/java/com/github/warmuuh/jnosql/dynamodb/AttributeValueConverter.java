package com.github.warmuuh.jnosql.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class AttributeValueConverter {

  private AttributeValueConverter() {

  }

  static Object toObject(AttributeValue value) {
    if (Boolean.TRUE.equals(value.nul())) {
      return null;
    }

    if (value.n() != null) {
      return value.n();
    }
    if (value.s() != null) {
      return value.s();
    }
    if (value.b() != null) {
      return value.b();
    }
    if (value.bool() != null) {
      return value.bool();
    }

    if (value.hasBs()) {
      return value.bs();
    }
    if (value.hasL()) {
      return value.l();
    }
    if (value.hasM()) {
      return value.m();
    }
    if (value.hasNs()) {
      return value.ns();
    }
    if (value.hasSs()) {
      return value.ss();
    }
    return null;
  }

  static AttributeValue fromObject(Object value) {
    if (value == null) {
      return AttributeValue.fromNul(true);
    }

    if (value instanceof String) {
      return AttributeValue.fromS((String) value);
    }

    if (value instanceof Number) {
      return AttributeValue.fromN(String.valueOf(value));
    }

//    if (value.hasBs()) {
//      return value.bs();
//    }

    if (value instanceof List<?>) {
      return AttributeValue.fromL(((List<?>) value).stream().map(AttributeValueConverter::fromObject).toList());
    }
    if (value instanceof Map<?, ?>) {
      return AttributeValue.fromM(((Map<?, ?>) value).entrySet().stream().collect(Collectors.toMap(
          e -> e.getKey().toString(),
          e -> fromObject(e.getValue())
      )));
    }
//    if (value.hasNs()) {
//      return value.ns();
//    }
//    if (value.hasSs()) {
//      return value.ss();
//    }

    throw new IllegalArgumentException("Cannot convert value of type " + value.getClass() + " to dynamodb type");
  }
}
