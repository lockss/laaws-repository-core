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
import org.apache.commons.lang3.*;
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

  /** Assert that Iterable has no elements */
  public void assertEmpty(Iterable iter) {
    assertNotNull(iter);
    assertFalse(iter.iterator().hasNext());
  }

  /** Assert that Iterator has no elements */
  public void assertEmpty(Iterator iter) {
    assertNotNull(iter);
    assertFalse(iter.hasNext());
  }

  /** Assert that the Executable throws an instnace of the expected class,
   * and that the expected pattern is found in the Throwable's message . */
  public <T extends Throwable> T assertThrowsMatch(Class<T> expectedType,
						   String pattern,
						   Executable executable) {
    return assertThrowsMatch(expectedType, pattern, executable, null);
  }

  /** Assert that the Executable throws an instnace of the expected class,
   * and that the expected pattern is found in the Throwable's message . */
  public <T extends Throwable> T assertThrowsMatch(Class<T> expectedType,
						   String pattern,
						   Executable executable,
						   String message) {
    T th = assertThrows(expectedType, executable, message);
    assertThat(message,
	       th.getMessage(), FindPattern.findPattern(pattern));
    return th;
  }

  /** Read a byte, fail with a detailed message if an IOException is
   * thrown. */
  int paranoidRead(InputStream in, String streamName, long cnt, long expLen,
		   String message) {
    try {
      return in.read();
    } catch (IOException e) {
      fail( ( buildPrefix(message) + "after " + cnt + " bytes" +
	      (expLen >= 0 ? " of " + expLen : "") +
	      ", " + streamName + " stream threw " + e.toString()),
	    e);
      // compiler doesn't know fail() doesn't return
      throw new IllegalStateException("can't happen");
    }
  }

  /** Assert that the two InputStreams return the same sequence of bytes,
   * of the expected length.  Displays a detailed message if a mistmatch is
   * found, one stream runs out before the other, the length doesn't match
   * or an IOException is thrown while reading. */
  public void assertSameBytes(InputStream expected,
			      InputStream actual,
			      long expLen) {
    assertSameBytes(expected, actual, expLen, null);
  }
  
  /** Assert that the two InputStreams return the same sequence of bytes.
   * Displays a detailed message if a mistmatch is found, one stream runs
   * out before the other, the length doesn't match or an IOException is
   * thrown while reading. */
  public void assertSameBytes(InputStream expected,
			      InputStream actual) {
    assertSameBytes(expected, actual, null);
  }
  
  /** Assert that the two InputStreams return the same sequence of bytes.
   * Displays a detailed message if a mistmatch is found, one stream runs
   * out before the other, the length doesn't match or an IOException is
   * thrown while reading. */
  public void assertSameBytes(InputStream expected,
			      InputStream actual,
			      String message) {
    assertSameBytes(expected, actual, -1, message);
  }

  /** Assert that the two InputStreams return the same sequence of bytes,
   * of the expected length.  Displays a detailed message if a mistmatch is
   * found, one stream runs out before the other, the length doesn't match
   * or an IOException is thrown while reading. */
  public void assertSameBytes(InputStream expected,
			      InputStream actual,
			      long expLen,
			      String message) {
    if (expected == actual) {
      throw new IllegalArgumentException("assertSameBytes() called with same stream for both expected and actual.");
    }
    // XXX This could obscure the byte count at which an error occurs
    if (!(expected instanceof BufferedInputStream)) {
      expected = new BufferedInputStream(expected);
    }
    if (!(actual instanceof BufferedInputStream)) {
      actual = new BufferedInputStream(actual);
    }
    long cnt = 0;
    int ch = paranoidRead(expected, "expected", cnt, expLen, message);
    while (-1 != ch) {
      int ch2 = paranoidRead(actual, "actual", cnt, expLen, message);
      if (-1 == ch2) {
	fail(buildPrefix(message) +
	     "actual stream ran out early, at byte position " + cnt);
      }
      cnt++;
      assertEquals(ch, ch2,
		   buildPrefix(message) + "at byte position " + cnt);
      ch = paranoidRead(expected, "expected", cnt, expLen, message);
    }

    int ch2 = paranoidRead(actual, "actual", cnt, expLen, message);
    if (-1 != ch2) {
      fail(buildPrefix(message) +
	   "expected stream ran out early, at byte position " + cnt);
    }
    if (expLen >= 0) {
      assertEquals(expLen, cnt, "Both streams were wrong length");
    }
  }
  
  static String buildPrefix(String message) {
    return (StringUtils.isNotBlank(message) ? message + " ==> " : "");
  }

  public void assertSameCharacters(Reader expected,
				   Reader actual,
				   String message) {
    assertSameCharacters(expected, actual, -1, message);
  }
  
  /** Assert that the two Readers return the same sequence of
   * characters */
  public void assertSameCharacters(Reader expected,
				   Reader actual) {
    assertSameCharacters(expected, actual, null);
  }
  
  /** Read a byte, fail with a detailed message if an IOException is
   * thrown. */
  int paranoidReadChar(Reader in, String streamName, long cnt, long expLen,
		       String message) {
    try {
      return in.read();
    } catch (IOException e) {
      fail( ( buildPrefix(message) + "after " + cnt + " chars" +
	      (expLen >= 0 ? " of " + expLen : "") +
	      ", " + streamName + " stream threw " + e.toString()),
	    e);
      // compiler doesn't know fail() doesn't return
      throw new IllegalStateException("can't happen");
    }
  }

  /** Assert that the two Readers return the same sequence of characters,
   * of the expected length.  Displays a detailed message if a mistmatch is
   * found, one stream runs out before the other, the length doesn't match
   * or an IOException is thrown while reading. */
  public void assertSameCharacters(Reader expected,
				   Reader actual,
				   long expLen,
				   String message) {
    if (expected == actual) {
      throw new IllegalArgumentException("assertSameBytes() called with same reader for both expected and actual.");
    }
    // XXX This could obscure the char count at which an error occurs
    if (!(expected instanceof BufferedReader)) {
      expected = new BufferedReader(expected);
    }
    if (!(actual instanceof BufferedReader)) {
      actual = new BufferedReader(actual);
    }
    long cnt = 0;
    int ch = paranoidReadChar(expected, "expected", cnt, expLen, message);
    while (-1 != ch) {
      int ch2 = paranoidReadChar(actual, "actual", cnt, expLen, message);
      if (-1 == ch2) {
	fail(buildPrefix(message) +
	     "actual stream ran out early, at char position " + cnt);
      }
      cnt++;
      assertEquals((char)ch, (char)ch2,
		   buildPrefix(message) + "at char position " + cnt);
      ch = paranoidReadChar(expected, "expected", cnt, expLen, message);
    }

    int ch2 = paranoidReadChar(actual, "actual", cnt, expLen, message);
    if (-1 != ch2) {
      fail(buildPrefix(message) +
	   "expected stream ran out early, at char position " + cnt);
    }
    if (expLen >= 0) {
      assertEquals(expLen, cnt, "Both streams were wrong length");
    }
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
      throws IOException {
    int len = Math.max(1, expected.length() * 2);
    char[] ca = new char[len];
    StringBuilder actual = new StringBuilder(expected.length());

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
    StringBuilder actual = new StringBuilder(expected.length());

    int n;
    while ((n = reader.read(ca)) != -1) {
      actual.append(ca, 0, n);
    }
    assertEquals("With buffer size " + bufsize + ",",
		 expected, actual.toString());
  }



  /** Regexp matcher to use with assertThat.  Pattern must match entire
   * input string.  <i>Eg</i>, <code>assertThat("reason", "abc123",
   * MatchPattern.matchPattern("a.*23"))</code> */
  // XXX Copied from hamcrest 2.0.  If we upgrade, this can be removed
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


  /** Regexp matcher to use with assertThat.  Succeeds if pattern matches
   * anywhere within input string.  <i>Eg</i>, <code>assertThat("reason",
   * "abc123", FindPattern.findPattern("c12"))</code> */
  // Modification of MatchPattern to allow substring match.  Likely still
  // needed even if upgrade to hamcrest 2.0
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


  /** Log the start of each test class */
  @BeforeAll
  public static void logTestClass(TestInfo info) {
    log.info("Start test class: " + info.getDisplayName());
  }

  /** Log the end of each test class */
  @AfterAll
  public static void logTestClassEnd(TestInfo info) {
    log.info("End test class: " + info.getDisplayName());
  }

  /** Log each test method */
  @BeforeEach
  public void logTest(TestInfo info) {
//     Optional meth = info.getTestMethod();
    log.info("Testcase: " + info.getDisplayName());
  }

  /** Called by the &#64;VariantTest mechanism to set up the named
   * variant */
  protected void setUpVariant(String vName) {
  }


}
