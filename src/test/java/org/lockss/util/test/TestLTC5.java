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
import java.util.*;
import java.util.stream.Stream;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.logging.*;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.bag.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.*;
import org.opentest4j.AssertionFailedError;


public class TestLTC5 extends LTC5 {
  private final static Log log = LogFactory.getLog(TestLTC5.class);

  InputStream stringInputStream(String str) {
    return new ReaderInputStream(new StringReader(str));
  }

  @Test
  public void testMatchPattern() {
    assertThat("123", MatchesPattern.matchesPattern("1.3"));
    assertThat("123", not(MatchesPattern.matchesPattern("1.32")));
    assertThat("string string", not(MatchesPattern.matchesPattern("g st")));
    assertThat("string string", MatchesPattern.matchesPattern(".*g st.*"));
    assertThat("string string", not(MatchesPattern.matchesPattern("xxx")));
  }

  @Test
  public void testFindPattern() {
    assertThat("123", FindPattern.findPattern("1.3"));
    assertThat("123", not(FindPattern.findPattern("1.32")));
    assertThat("string string", FindPattern.findPattern("g st"));
    assertThat("string string", FindPattern.findPattern(".*g st.*"));
    assertThat("string string", not(FindPattern.findPattern("xxx")));
  }

  static String STREAM_CLOSED_PAT =
    "java\\.io\\.IOException: [Ss]tream ?[Cc]losed";

  @Test
  public void testAssertSameBytes() {
    assertThrowsMatch(AssertionFailedError.class,
		      "expected stream ran out early, at byte position 3",
		      () -> {assertSameBytes(stringInputStream("123"),
					     stringInputStream("1234"));});
    assertThrowsMatch(AssertionFailedError.class,
		      "actual stream ran out early, at byte position 4",
		      () -> {assertSameBytes(stringInputStream("foooo"),
					     stringInputStream("fooo"));});

    assertThrowsMatch(AssertionFailedError.class,
		      "at byte position 4 ==> expected: <52> but was: <53>",
		      () -> {assertSameBytes(stringInputStream("12345"),
					     stringInputStream("12354"));});
    InputStream closedStream = stringInputStream("123");
    try {
      closedStream.close();
    } catch (IOException e) {
      fail("Couldn't close stream", e);
    }
    assertThrowsMatch(AssertionFailedError.class,
		      "after 0 bytes, expected stream threw " + STREAM_CLOSED_PAT,
		      () -> {assertSameBytes(closedStream,
					     stringInputStream("foo"));});
    assertThrowsMatch(AssertionFailedError.class,
		      "after 0 bytes, actual stream threw " + STREAM_CLOSED_PAT,
		      () -> {assertSameBytes(stringInputStream("foo"),
					     closedStream);});
  }

  @Test
  public void testAssertSameCharacters() {
    assertThrowsMatch(AssertionFailedError.class,
		      "expected stream ran out early, at char position 3",
		      () -> {assertSameCharacters(new StringReader("foo"),
						  new StringReader("fooo"));});
    assertThrowsMatch(AssertionFailedError.class,
		      "actual stream ran out early, at char position 4",
		      () -> {assertSameCharacters(new StringReader("foooo"),
						  new StringReader("fooo"));});

    assertThrowsMatch(AssertionFailedError.class,
		      "at char position 4 ==> expected: <6> but was: <o>",
		      () -> {assertSameCharacters(new StringReader("foo6"),
						  new StringReader("fooo"));});
    Reader closedRdr = new StringReader("123");
    try {
      closedRdr.close();
    } catch (IOException e) {
      fail("Couldn't close reader", e);
    }
    assertThrowsMatch(AssertionFailedError.class,
		      "after 0 chars, expected stream threw " + STREAM_CLOSED_PAT,
		      () -> {assertSameCharacters(closedRdr,
						  new StringReader("foo"));});
    assertThrowsMatch(AssertionFailedError.class,
		      "after 0 chars, actual stream threw " + STREAM_CLOSED_PAT,
		      () -> {assertSameCharacters(new StringReader("foo"),
						  closedRdr);});
  }
}


