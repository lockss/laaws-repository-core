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

import static org.junit.jupiter.params.aggregator.AggregationUtils.hasAggregator;
import static org.junit.platform.commons.util.AnnotationUtils.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.logging.*;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.aggregator.AggregationUtils;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.AnnotationConsumerInitializer;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.commons.util.ReflectionUtils;

/**
 * @since 5.0
 */
class VariantTestExtension implements TestTemplateInvocationContextProvider {
  private final static Log log = LogFactory.getLog(TestVariantTest.class);

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    if (!context.getTestMethod().isPresent()) {
      return false;
    }

    Method testMethod = context.getTestMethod().get();
    if (!isAnnotated(testMethod, VariantTest.class)) {
      return false;
    }

    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext>
    provideTestTemplateInvocationContexts(ExtensionContext context) {
    Method templateMethod = context.getRequiredTestMethod();
    VariantTestNameFormatter formatter = createNameFormatter(templateMethod);
    AtomicLong invocationCount = new AtomicLong(0);
    return findRepeatableAnnotations(templateMethod, ArgumentsSource.class)
      .stream()
      .map(ArgumentsSource::value)
      .map(ReflectionUtils::newInstance)
      .map(provider -> AnnotationConsumerInitializer.initialize(templateMethod,
								provider))
      .flatMap(provider -> arguments(provider, context))
      .map(Arguments::get)
      .flatMap(a -> Stream.of(a))
      .map(vName -> createInvocationContext(formatter, vName.toString()))
      .peek(invocationContext -> invocationCount.incrementAndGet())
      .onClose(() ->
	       Preconditions.condition(invocationCount.get() > 0,
				       "Configuration error: You must provide at least one argument for this @VariantTest"));
  }

  private TestTemplateInvocationContext
    createInvocationContext(VariantTestNameFormatter formatter, String vName) {
    return new VariantTestInvocationContext(formatter, vName);
  }

  private VariantTestNameFormatter createNameFormatter(Method templateMethod) {
    VariantTest variantTest = findAnnotation(templateMethod,
					     VariantTest.class).get();
    String name =
      Preconditions.notBlank(variantTest.name().trim(),
			     () -> String.format("Configuration error: @VariantTest on method [%s] must be declared with a non-empty name.",
						 templateMethod));
    return new VariantTestNameFormatter(templateMethod, name);
  }

  protected static Stream<? extends Arguments>
    arguments(ArgumentsProvider provider, ExtensionContext context) {
    try {
      return provider.provideArguments(context);
    }
    catch (Exception e) {
      throw ExceptionUtils.throwAsUncheckedException(e);
    }
  }

}
