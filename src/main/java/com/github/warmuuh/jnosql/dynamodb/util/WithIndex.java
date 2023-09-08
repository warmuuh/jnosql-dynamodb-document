package com.github.warmuuh.jnosql.dynamodb.util;

import java.util.function.Function;

public class WithIndex<T> {
  private int index;
  private T value;

  WithIndex(int index, T value) {
    this.index = index;
    this.value = value;
  }

  public int index() {
    return index;
  }

  public T value() {
    return value;
  }

  @Override
  public String toString() {
    return value + "(" + index + ")";
  }

  public static <T> Function<T, WithIndex<T>> indexed() {
    return new Function<T, WithIndex<T>>() {
      int index = 0;
      @Override
      public WithIndex<T> apply(T t) {
        return new WithIndex<>(index++, t);
      }
    };
  }
}