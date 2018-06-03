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
import org.apache.commons.logging.*;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.bag.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.*;


public class TestVariantTest extends LTC5 {
  private final static Log log = LogFactory.getLog(TestVariantTest.class);

  // Name of currently running variant, stored by variant mechanism
  private String tmpVariant = "no variant";
  // Name of currently running variant, stored by @BeforeEach
  private String variant = "no variant";
  // Record all variants run
  private static Bag testsRun = new HashBag();

  // Expected variants run
  static Bag expTests = new HashBag(Arrays.asList(new String[] {
	"testOne_var_one",
	"testOne_var_two",
	"testTwo_var_one",
	"testTwo_var_two",
	"testEnum_var_one",
	"testEnum_var_two",
	"testEnum_abnormal",
	"testEnum_atypical",
	"testEnum_anomalous",
	"testEnum_irregular",
	"testMethod_gen_a",
	"testMethod_gen_b",
      }));

  @AfterAll
  public static void after() {
    Assertions.assertEquals(expTests, testsRun);
  }

  // Required by variant mechanism, runs before @BeforeEach
  protected void setUpVariant(String variantName) {
    log.info("setUpVariant: " + variantName);
    tmpVariant = variantName;
  }

  @BeforeEach
  // Copy variant to demonstrate that @BeforeEach methods run after the
  // variant is set up
  public void bVariant() {
    variant = tmpVariant;
    log.info("before each, variant: " + variant);
  }

  @BeforeEach
  public void cRec(TestInfo info) {
    log.debug("Test: " + info.getDisplayName());
  }

  // No variants
  @Test
  public void t1() {
    log.info("t1");
  }


  @VariantTest
  @ValueSource(strings = { "var_one", "var_two" })
  void testOne() {
    testsRun.add("testOne_" + variant);
    log.info("testOne, variant: " + variant);
  }

  @VariantTest
  @ValueSource(strings = { "var_one", "var_two" })
  void testTwo() {
    testsRun.add("testTwo_" + variant);
    log.info("testTwo, variant: " + variant);
  }

  enum DeviantVariants {
    abnormal, atypical, anomalous, irregular,
  };

  @VariantTest
  @ValueSource(strings = { "var_one", "var_two" })
  @EnumSource(DeviantVariants.class)
  void testEnum() {
    testsRun.add("testEnum_" + variant);
    log.info("testEnum, variant: " + variant);
  }

  static Stream<String> genVariants() {
    return Stream.of("gen_a", "gen_b");
  }

  @VariantTest
  @MethodSource("genVariants")
  void testMethod() {
    testsRun.add("testMethod_" + variant);
    log.info("testEnum, variant: " + variant);
  }

}
