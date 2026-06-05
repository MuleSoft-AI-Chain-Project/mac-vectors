package org.mule.extension.vectors.internal.helper.metadata;

import static dev.langchain4j.store.embedding.filter.MetadataFilterBuilder.metadataKey;

import org.mule.extension.vectors.internal.constant.Constants;
import org.mule.extension.vectors.internal.util.Utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.MetadataFilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataFilterHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataFilterHelper.class);

  // @SuppressWarnings("java:S5852") - Regex patterns are optimized with possessive quantifiers to avoid backtracking
  private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)");
  private static final Pattern OPERATOR_PATTERN = Pattern.compile("([=!><]+)");
  // Fixed regex to avoid catastrophic backtracking by using possessive quantifiers
  private static final Pattern CONTAINS_FUNCTION_PATTERN =
      Pattern.compile("CONTAINS\\s*+\\(\\s*+([a-zA-Z_][a-zA-Z0-9_]*)\\s*+,\\s*+([^)]*+)\\s*+\\)");

  private static final Pattern NUMBER_PATTERN =
      Pattern.compile("^-?\\d+(\\.\\d+)?$");

  private enum OperatorType {
    AND, OR, NONE
  }

  public static Filter fromExpression(String expression) {
    validateExpression(expression);
    expression = stripRedundantParentheses(expression);
    List<String> tokens = splitExpression(expression);
    LOGGER.debug("splitExpression tokens for '{}': {}", expression, tokens);

    if (tokens.size() > 1) {
      return handleCompositeExpression(expression, tokens);
    }

    if (isParenthesizedSingleToken(expression)) {
      LOGGER.debug("Processing single token as parenthesized sub-expression: {}", expression);
      return fromExpression(expression.substring(1, expression.length() - 1).trim());
    }

    return parseSimpleCondition(expression);
  }

  private static void validateExpression(String expression) {
    if (expression == null || expression.trim().isEmpty()) {
      throw new IllegalArgumentException("Expression cannot be null or empty");
    }
  }

  private static String stripRedundantParentheses(String expression) {
    expression = expression.trim();
    LOGGER.debug("Processing expression: {}", expression);
    while (expression.startsWith("(") && expression.endsWith(")") && isRedundantlyWrapped(expression)) {
      LOGGER.debug("Stripping redundant outer parentheses from: {}", expression);
      expression = expression.substring(1, expression.length() - 1).trim();
      LOGGER.debug("Expression after stripping: {}", expression);
    }
    return expression;
  }

  private static boolean isParenthesizedSingleToken(String expression) {
    return expression.startsWith("(") && expression.endsWith(")") && isRedundantlyWrapped(expression);
  }

  private static Filter handleCompositeExpression(String expression, List<String> tokens) {
    OperatorType opType = getOperatorTypeForCurrentLevel(expression);
    boolean currentLevelIsAnd;
    if (opType == OperatorType.AND) {
      currentLevelIsAnd = true;
    } else if (opType == OperatorType.OR) {
      currentLevelIsAnd = false;
    } else {
      throw new IllegalArgumentException(String.format(
                                                       "Could not determine consistent logical operator for expression: %s. Tokens found: %d",
                                                       expression, tokens.size()));
    }
    List<Filter> subFilters = new ArrayList<>();
    for (String token : tokens) {
      if (token.trim().isEmpty()) {
        throw new IllegalArgumentException(String.format("Empty condition in expression: %s", expression));
      }
      subFilters.add(fromExpression(token.trim()));
    }
    return createCompositeFilter(subFilters, currentLevelIsAnd);
  }

  private static Filter parseSimpleCondition(String expression) {
    // First check if this is a CONTAINS function call
    Matcher containsMatcher = CONTAINS_FUNCTION_PATTERN.matcher(expression);
    if (containsMatcher.matches()) {
      String field = containsMatcher.group(1);
      String valueStr = containsMatcher.group(2).trim();
      // Handle quoted strings
      if ((valueStr.startsWith("'") && valueStr.endsWith("'")) ||
          (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
        valueStr = valueStr.substring(1, valueStr.length() - 1);
      }
      Object value = parseValue(valueStr);
      return createFilter(field, "CONTAINS", value);
    }

    // Parse regular condition: field operator value
    String trimmedExpression = expression.trim();

    // Find field name
    Matcher fieldMatcher = FIELD_NAME_PATTERN.matcher(trimmedExpression);
    if (!fieldMatcher.find()) {
      throw new IllegalArgumentException(String.format("Invalid field name in condition: %s", expression));
    }
    String field = fieldMatcher.group(1);
    int fieldEnd = fieldMatcher.end();

    // Find operator
    String remainingAfterField = trimmedExpression.substring(fieldEnd).trim();
    Matcher operatorMatcher = OPERATOR_PATTERN.matcher(remainingAfterField);
    if (!operatorMatcher.find()) {
      throw new IllegalArgumentException(String.format("Invalid operator in condition: %s", expression));
    }
    String operator = operatorMatcher.group(1);
    int operatorEnd = operatorMatcher.end();

    // Get value
    String valueStr = remainingAfterField.substring(operatorEnd).trim();
    if (valueStr.isEmpty()) {
      throw new IllegalArgumentException(String.format("Missing value in condition: %s", expression));
    }

    // Handle quoted strings
    if ((valueStr.startsWith("'") && valueStr.endsWith("'")) ||
        (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
      valueStr = valueStr.substring(1, valueStr.length() - 1);
    }

    // Parse value
    Object value = parseValue(valueStr);
    return createFilter(field, operator, value);
  }

  private static boolean isRedundantlyWrapped(String expression) {
    if (!expression.startsWith("(") || !expression.endsWith(")")) {
      return false;
    }
    int balance = 0;
    for (int i = 0; i < expression.length() - 1; i++) {
      if (expression.charAt(i) == '(') {
        balance++;
      } else if (expression.charAt(i) == ')') {
        balance--;
      }
      if (balance == 0 && i < expression.length() - 1) {
        return false;
      }
    }
    return balance == 1;
  }

  private static OperatorType getOperatorTypeForCurrentLevel(String expression) {
    int balance = 0;
    for (int i = 0; i < expression.length(); i++) {
      char c = expression.charAt(i);
      if (c == '(') {
        balance++;
      } else if (c == ')') {
        balance--;
        if (balance < 0) {
          throw new IllegalArgumentException(String
              .format("Mismatched parentheses leading to negative balance at index %d in: %s", i, expression));
        }
      } else if (balance == 0) {
        if (checkLogicalOperator(expression, i, "AND")) {
          return OperatorType.AND;
        }
        if (checkLogicalOperator(expression, i, "OR")) {
          return OperatorType.OR;
        }
      }
    }
    return OperatorType.NONE;
  }

  private static Object parseValue(String value) {
    if (NUMBER_PATTERN.matcher(value).matches()) {
      try {
        if (value.contains(".")) {
          return Double.parseDouble(value);
        } else {
          return Long.parseLong(value);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format("Invalid number format for value '%s' that matched number pattern.",
                                                         value));
      }
    }
    return value;
  }

  private static Filter createFilter(String field, String operator, Object value) {

    String methodName = "";
    try {
      MetadataFilterBuilder filterBuilder = metadataKey(field);

      // Handle CONTAINS operator specially
      if ("CONTAINS".equals(operator)) {
        String stringValue = value.toString();
        // Remove quotes if present
        if ((stringValue.startsWith("'") && stringValue.endsWith("'")) ||
            (stringValue.startsWith("\"") && stringValue.endsWith("\""))) {
          stringValue = stringValue.substring(1, stringValue.length() - 1);
        }
        return filterBuilder.containsString(stringValue);
      }

      methodName = getFilterMethod(operator);
      if (value == null) {
        throw new IllegalArgumentException(String.format("Value cannot be null for operator %s on field %s", operator, field));
      }
      Method method = filterBuilder.getClass().getMethod(methodName, Utils.getPrimitiveTypeClass(value)); // Line 187 from stack trace
      return (Filter) method.invoke(filterBuilder, value);

    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException nsme) {
      // Provide more info about the expected parameter type based on Utils.getPrimitiveTypeClass(value)
      Class<?> expectedParamType = Utils.getPrimitiveTypeClass(value);
      String expectedParamTypeName = expectedParamType.getName();

      throw new IllegalArgumentException(String.format(
                                                       "Failed to create filter. Method '%s' with parameter type '%s' not found. Details: %s",
                                                       methodName, expectedParamTypeName, nsme.getMessage()));
    }
  }

  private static String getFilterMethod(String operator) {
    switch (operator) {
      case "=":
        return Constants.METADATA_FILTER_METHOD_IS_EQUAL_TO;
      case "!=":
        return Constants.METADATA_FILTER_METHOD_IS_NOT_EQUAL_TO;
      case ">":
        return Constants.METADATA_FILTER_METHOD_IS_GREATER_THAN;
      case ">=":
        return Constants.METADATA_FILTER_METHOD_IS_GREATER_THAN_OR_EQUAL_TO;
      case "<":
        return Constants.METADATA_FILTER_METHOD_IS_LESS_THAN;
      case "<=":
        return Constants.METADATA_FILTER_METHOD_IS_LESS_THAN_OR_EQUAL_TO;
      default:
        throw new IllegalArgumentException(String.format("Unsupported operator: %s", operator));
    }
  }

  private static List<String> splitExpression(String expression) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int openParens = 0;
    QuoteState quoteState = new QuoteState();
    OperatorState operatorState = new OperatorState();

    int i = 0;
    while (i < expression.length()) {
      char c = expression.charAt(i);
      updateQuoteState(quoteState, c);

      if (!quoteState.inSingleQuote && !quoteState.inDoubleQuote) {
        openParens = updateParenthesesBalance(openParens, c, expression);

        if (openParens == 0 && isPotentialOperatorStart(c)) {
          OperatorType foundOp = detectLogicalOperator(expression, i);
          if (foundOp != OperatorType.NONE) {
            validateOperatorState(operatorState, foundOp, expression);
            addCurrentTokenIfNotEmpty(parts, current);
            i = skipOperatorAndContinue(expression, i, foundOp);
            continue;
          }
        }
      }
      current.append(c);
      i++;
    }

    validateFinalParenthesesBalance(openParens, expression);
    addRemainingToken(parts, current, expression);
    return parts;
  }

  private static boolean isPotentialOperatorStart(char c) {
    return Character.isUpperCase(c) || Character.isLetter(c);
  }

  private static int updateParenthesesBalance(int openParens, char c, String expression) {
    if (c == '(') {
      return openParens + 1;
    } else if (c == ')') {
      int newBalance = openParens - 1;
      if (newBalance < 0) {
        throw new IllegalArgumentException("Mismatched parentheses in expression (extra closing): " + expression);
      }
      return newBalance;
    }
    return openParens;
  }

  private static void addCurrentTokenIfNotEmpty(List<String> parts, StringBuilder current) {
    if (current.length() > 0) {
      parts.add(current.toString().trim());
      current.setLength(0);
    }
  }

  private static int skipOperatorAndContinue(String expression, int currentIndex, OperatorType foundOp) {
    int skipLength = (foundOp == OperatorType.AND ? "AND".length() : "OR".length());
    for (int skip = 1; skip < skipLength; skip++) {
      if (currentIndex + skip < expression.length()) {
        // Skip the operator characters
      }
    }
    return currentIndex + skipLength;
  }

  private static void validateFinalParenthesesBalance(int openParens, String expression) {
    if (openParens != 0) {
      throw new IllegalArgumentException("Mismatched parentheses in expression (extra opening): " + expression);
    }
  }

  private static void addRemainingToken(List<String> parts, StringBuilder current, String expression) {
    if (current.length() > 0) {
      parts.add(current.toString().trim());
    }

    if (parts.isEmpty() && !expression.trim().isEmpty()) {
      parts.add(expression.trim());
    }
  }

  private static boolean checkLogicalOperator(String expression, int index, String operator) {
    if (index + operator.length() > expression.length()) { // Boundary check
      return false;
    }
    // Case-insensitive check for the operator itself
    if (!expression.substring(index, index + operator.length()).equalsIgnoreCase(operator)) {
      return false;
    }
    // Check char before operator (if not start of string)
    if (index > 0 && !Character.isWhitespace(expression.charAt(index - 1))) {
      return false;
    }
    // Check char after operator (if not end of string)
    if ((index + operator.length()) < expression.length() &&
        !Character.isWhitespace(expression.charAt(index + operator.length()))) {
      return false;
    }
    return true;
  }

  private static Filter createCompositeFilter(List<Filter> subFilters, boolean isAnd) {
    if (subFilters.isEmpty()) {
      throw new IllegalArgumentException("Cannot create composite filter with no subfilters");
    }
    if (subFilters.size() == 1) {
      return subFilters.get(0);
    }

    Filter result = subFilters.get(0);
    for (int i = 1; i < subFilters.size(); i++) {
      if (isAnd) {
        result = result.and(subFilters.get(i));
      } else {
        result = result.or(subFilters.get(i));
      }
    }
    return result;
  }

  private static class QuoteState {

    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
  }

  private static void updateQuoteState(QuoteState state, char c) {
    if (c == '\'' && !state.inDoubleQuote) {
      state.inSingleQuote = !state.inSingleQuote;
    } else if (c == '"' && !state.inSingleQuote) {
      state.inDoubleQuote = !state.inDoubleQuote;
    }
  }

  private static class OperatorState {

    boolean firstOperatorIsAnd = false;
    boolean firstOperatorIsOr = false;
    boolean firstOperatorFound = false;
  }

  private static OperatorType detectLogicalOperator(String expression, int i) {
    if (checkLogicalOperator(expression, i, "AND")) {
      return OperatorType.AND;
    }
    if (checkLogicalOperator(expression, i, "OR")) {
      return OperatorType.OR;
    }
    return OperatorType.NONE;
  }

  private static void validateOperatorState(OperatorState state, OperatorType foundOp, String expression) {
    if (!state.firstOperatorFound) {
      state.firstOperatorIsAnd = (foundOp == OperatorType.AND);
      state.firstOperatorIsOr = (foundOp == OperatorType.OR);
      state.firstOperatorFound = true;
    } else {
      if ((state.firstOperatorIsAnd && foundOp == OperatorType.OR) || (state.firstOperatorIsOr && foundOp == OperatorType.AND)) {
        throw new IllegalArgumentException("Mixed AND/OR operations at the same level must be explicitly grouped with parentheses. Expression: "
            + expression);
      }
    }
  }
}
