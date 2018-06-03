/*
 * Copyright 2015-2018 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * http://www.eclipse.org/legal/epl-v20.html
 */



package org.lockss.util.test;

import java.lang.reflect.Method;
import static java.util.stream.Collectors.joining;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.StringUtils;

class VariantTestNameFormatter {

  private final String methodName;
  private final String namePattern;

  VariantTestNameFormatter(Method templateMethod, String namePattern) {
    this.namePattern = namePattern;
    this.methodName = templateMethod.getName();
  }

  String format(int invocationIndex, Object... arguments) {
    String pattern = prepareMessageFormatPattern(invocationIndex, arguments);
    Object[] humanReadableArguments = makeReadable(arguments);
    return formatSafely(pattern, humanReadableArguments);
  }

  private String prepareMessageFormatPattern(int invocationIndex,
					     Object[] arguments) {
    String result = namePattern.replace("{index}",
					String.valueOf(invocationIndex));
    if (result.contains("{arguments}")) {
      String replacement = IntStream.range(0, arguments.length)
	.mapToObj(index -> "{" + index + "}")
	.collect(joining(", "));
      result = result.replace("{arguments}", replacement + " variant");
    }
    if (namePattern != null) {
      result = methodName + "() " + result;
    }
    return result;
  }

  private Object[] makeReadable(Object[] arguments) {
    // Note: humanReadableArguments must be an Object[] in order to
    // avoid varargs issues with non-Eclipse compilers.
    Object[] humanReadableArguments = //
      Arrays.stream(arguments).map(StringUtils::nullSafeToString).toArray(String[]::new);
    return humanReadableArguments;
  }

  private String formatSafely(String pattern, Object[] arguments) {
    try {
      return MessageFormat.format(pattern, arguments);
    }
    catch (IllegalArgumentException ex) {
      String message = "The naming pattern defined for the variant tests is invalid. "
	+ "The nested exception contains more details.";
      throw new JUnitException(message, ex);
    }
  }

}
