package org.mule.extension.vectors.internal.helper.metadata;

import static org.assertj.core.api.Assertions.*;

import dev.langchain4j.store.embedding.filter.Filter;
import org.junit.jupiter.api.Test;

class MetadataFilterHelperTest {

  @Test
  void fromExpression_shouldParseSimpleEquality() {
    Filter filter = MetadataFilterHelper.fromExpression("foo = 'bar'");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsEqualTo").contains("foo").contains("bar");
  }

  @Test
  void fromExpression_shouldParseSimpleInequality() {
    Filter filter = MetadataFilterHelper.fromExpression("foo != 'baz'");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsNotEqualTo").contains("foo").contains("baz");
  }

  @Test
  void fromExpression_shouldParseNumericComparison() {
    Filter filter = MetadataFilterHelper.fromExpression("num > 10");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsGreaterThan").contains("num").contains("10");
  }

  @Test
  void fromExpression_shouldParseContainsFunction() {
    Filter filter = MetadataFilterHelper.fromExpression("CONTAINS(foo, 'bar')");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("ContainsString").contains("foo").contains("bar");
  }

  @Test
  void fromExpression_shouldParseAndOrComposite() {
    Filter filter = MetadataFilterHelper.fromExpression("foo = 'bar' AND num > 5");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("And").contains("IsEqualTo").contains("IsGreaterThan")
        .contains("key=foo, comparisonValue=bar")
        .contains("key=num, comparisonValue=5");
  }

  @Test
  void fromExpression_shouldParseParentheses() {
    Filter filter = MetadataFilterHelper.fromExpression("(foo = 'bar' OR num < 3)");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Or").contains("IsEqualTo").contains("IsLessThan")
        .contains("key=foo, comparisonValue=bar")
        .contains("key=num, comparisonValue=3");
  }

  @Test
  void fromExpression_shouldThrowOnNullOrEmpty() {
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression(null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromExpression_shouldThrowOnInvalidSyntax() {
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("foo === 'bar'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported operator");
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("foo ="))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromExpression_greaterThanOrEqual() {
    Filter filter = MetadataFilterHelper.fromExpression("score >= 50");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsGreaterThanOrEqualTo");
  }

  @Test
  void fromExpression_lessThan() {
    Filter filter = MetadataFilterHelper.fromExpression("age < 30");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsLessThan");
  }

  @Test
  void fromExpression_lessThanOrEqual() {
    Filter filter = MetadataFilterHelper.fromExpression("price <= 100.5");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsLessThanOrEqualTo");
  }

  @Test
  void fromExpression_multipleAndConditions() {
    Filter filter = MetadataFilterHelper.fromExpression("a = 'x' AND b = 'y' AND c = 'z'");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("And")
        .contains("key=a, comparisonValue=x")
        .contains("key=b, comparisonValue=y")
        .contains("key=c, comparisonValue=z");
  }

  @Test
  void fromExpression_multipleOrConditions() {
    Filter filter = MetadataFilterHelper.fromExpression("a = 'x' OR b = 'y' OR c = 'z'");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Or")
        .contains("key=a, comparisonValue=x")
        .contains("key=b, comparisonValue=y")
        .contains("key=c, comparisonValue=z");
  }

  @Test
  void fromExpression_nestedParentheses() {
    Filter filter = MetadataFilterHelper.fromExpression("(a = 'x' AND b = 'y') OR (c = 'z')");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Or")
        .contains("key=a, comparisonValue=x")
        .contains("key=b, comparisonValue=y")
        .contains("key=c, comparisonValue=z");
  }

  @Test
  void fromExpression_doubleNestedParentheses() {
    Filter filter = MetadataFilterHelper.fromExpression("((a = 'x'))");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsEqualTo");
  }

  @Test
  void fromExpression_containsWithDoubleQuotes() {
    Filter filter = MetadataFilterHelper.fromExpression("CONTAINS(name, \"hello\")");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("ContainsString")
        .contains("key=name, comparisonValue=hello");
  }

  @Test
  void fromExpression_numericDoubleValue() {
    Filter filter = MetadataFilterHelper.fromExpression("score = 3.14");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("IsEqualTo");
  }

  @Test
  void fromExpression_negativeNumber() {
    Filter filter = MetadataFilterHelper.fromExpression("temp > -5");
    assertThat(filter).isNotNull();
  }

  @Test
  void fromExpression_stringValueWithDoubleQuotes() {
    Filter filter = MetadataFilterHelper.fromExpression("name = \"John\"");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("John");
  }

  @Test
  void fromExpression_throwsOnMixedAndOr() {
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("a = 'x' AND b = 'y' OR c = 'z'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Mixed AND/OR");
  }

  @Test
  void fromExpression_throwsOnMismatchedParentheses() {
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("(a = 'x'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("parentheses");
  }

  @Test
  void fromExpression_throwsOnExtraClosingParenthesis() {
    assertThatThrownBy(() -> MetadataFilterHelper.fromExpression("a = 'x')"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromExpression_longIntegerValue() {
    Filter filter = MetadataFilterHelper.fromExpression("count = 9999999999");
    assertThat(filter).isNotNull();
  }

  @Test
  void fromExpression_equalityWithUnquotedStringValue() {
    Filter filter = MetadataFilterHelper.fromExpression("status = active");
    assertThat(filter).isNotNull();
  }

  @Test
  void fromExpression_complexNestedExpression() {
    Filter filter = MetadataFilterHelper.fromExpression("(a = 1 AND b = 2) OR (c = 3 AND d = 4)");
    assertThat(filter).isNotNull();
    assertThat(filter.toString()).contains("Or").contains("And")
        .contains("key=a, comparisonValue=1")
        .contains("key=b, comparisonValue=2")
        .contains("key=c, comparisonValue=3")
        .contains("key=d, comparisonValue=4");
  }
}
