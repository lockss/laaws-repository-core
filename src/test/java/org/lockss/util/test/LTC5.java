/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.util.test;

import java.io.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

import javax.xml.namespace.NamespaceContext;

import org.apache.commons.io.*;
import org.apache.commons.logging.*;

import org.hamcrest.*;
import org.hamcrest.collection.IsArray;
import org.hamcrest.core.*;
import org.hamcrest.text.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.*;
import org.junit.jupiter.params.provider.*;
import org.opentest4j.MultipleFailuresError;
import org.w3c.dom.Node;



import java.util.regex.Pattern;

public class LTC5 extends LockssTestCase5 {
  private final static Log log = LogFactory.getLog(LTC5.class);

  public void assertEmpty(Iterable iter) {
    assertNotNull(iter);
    assertFalse(iter.iterator().hasNext());
  }

  public void assertEmpty(Iterator iter) {
    assertNotNull(iter);
    assertFalse(iter.hasNext());
  }

  public <T extends Throwable> T assertThrowsMatch(Class<T> expectedType,
						   String pattern,
						   Executable executable) {
    return assertThrowsMatch(expectedType, pattern, executable, null);
  }

  public <T extends Throwable> T assertThrowsMatch(Class<T> expectedType,
						   String pattern,
						   Executable executable,
						   String message) {
    T th = assertThrows(expectedType, executable, message);
    assertThat(message,
	       th.getMessage(), FindPattern.findPattern(pattern));
    //     assertThat(th.getMessage(), equals(pattern));
    return th;
  }

  public void assertSameBytes(InputStream expected,
                                     InputStream actual,
				     String message)
      throws IOException {
    if (!IOUtils.contentEquals(expected, actual)) {
      if (message == null) {
        fail("Bytes not same");
      }
      else {
        fail(message);
      }
    }
  }
  
  public void assertSameBytes(InputStream expected,
			      InputStream actual)
      throws IOException {
    assertSameBytes(expected, actual, null);
  }
  
  public void assertSameCharacters(Reader expected,
				   Reader actual,
				   String message)
      throws IOException {
    if (!IOUtils.contentEquals(expected, actual)) {
      if (message == null) {
        fail("Characters not same");
      }
      else {
        fail(message);
      }
    }
  }
  
  public void assertSameCharacters(Reader expected,
				   Reader actual)
      throws IOException {
    assertSameCharacters(expected, actual, null);
  }
  
  /**
   * Asserts that a string matches the content of an InputStream
   */
  public void assertInputStreamMatchesString(String expected,
						    InputStream in)
      throws IOException {
    assertInputStreamMatchesString(expected, in, "UTF-8");
  }

  /**
   * Asserts that a string matches the content of an InputStream
   */
  public void assertInputStreamMatchesString(String expected,
						    InputStream in,
						    String encoding)
      throws IOException {
    Reader rdr = new InputStreamReader(in, encoding);
    assertReaderMatchesString(expected, rdr);
  }

  /**
   * Asserts that a string matches the content of a reader read using the
   * specified buffer size.
   */
  public void assertInputStreamMatchesString(String expected,
						    InputStream in,
						    int bufsize)
      throws IOException {
    Reader rdr = new InputStreamReader(in, "UTF-8");
    assertReaderMatchesString(expected, rdr, bufsize);
  }

  /**
   * Asserts that a string matches the content of a reader
   */
  public void assertReaderMatchesString(String expected, Reader reader)
      throws IOException{
    int len = Math.max(1, expected.length() * 2);
    char[] ca = new char[len];
    StringBuffer actual = new StringBuffer(expected.length());

    int n;
    while ((n = reader.read(ca)) != -1) {
      actual.append(ca, 0, n);
    }
    assertEquals(expected, actual.toString());
  }

  /**
   * Asserts that a string matches the content of a reader read using the
   * specified buffer size.
   */
  public void assertReaderMatchesString(String expected, Reader reader,
					       int bufsize)
      throws IOException {
    char[] ca = new char[bufsize];
    StringBuffer actual = new StringBuffer(expected.length());

    int n;
    while ((n = reader.read(ca)) != -1) {
      actual.append(ca, 0, n);
    }
    assertEquals("With buffer size " + bufsize + ",",
		 expected, actual.toString());
  }



  public static class MatchesPattern extends TypeSafeMatcher<String> {
    private final Pattern pattern;

    public MatchesPattern(Pattern pattern) {
      this.pattern = pattern;
    }

    @Override
    protected boolean matchesSafely(String item) {
      return pattern.matcher(item).matches();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a string matching the pattern '" + pattern + "'");
    }

    /**
     * Creates a matcher of {@link java.lang.String} that matches when the examined string
     * exactly matches the given {@link java.util.regex.Pattern}.
     */
    public static Matcher<String> matchesPattern(Pattern pattern) {
      return new MatchesPattern(pattern);
    }

    /**
     * Creates a matcher of {@link java.lang.String} that matches when the examined string
     * exactly matches the given regular expression, treated as a {@link java.util.regex.Pattern}.
     */
    public static Matcher<String> matchesPattern(String regex) {
      return new MatchesPattern(Pattern.compile(regex));
    }
  }


  public static class FindPattern extends TypeSafeMatcher<String> {
    private final Pattern pattern;

    public FindPattern(Pattern pattern) {
      this.pattern = pattern;
    }

    @Override
    protected boolean matchesSafely(String item) {
      return pattern.matcher(item).find();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a string containing the pattern '" + pattern + "'");
    }

    /**
     * Creates a matcher of {@link java.lang.String} that matches when the
     * pattern is contained in the examined string
     */
    public static Matcher<String> findPattern(Pattern pattern) {
      return new FindPattern(pattern);
    }

    /**
     * Creates a matcher of {@link java.lang.String} that matches when the
     * regex is contained in the examined string
     */
    public static Matcher<String> findPattern(String regex) {
      return new FindPattern(Pattern.compile(regex));
    }
  }


  @BeforeAll
  public static void logTestClass(TestInfo info) {
    log.info("Start test class: " + info.getDisplayName());
  }

  @AfterAll
  public static void logTestClassEnd(TestInfo info) {
    log.info("End test class: " + info.getDisplayName());
  }

  @BeforeEach
  public void logTest(TestInfo info) {
//     Optional meth = info.getTestMethod();
    log.info("Testcase: " + info.getDisplayName());
  }

  protected void setUpVariant(String vName) {
  }


}
