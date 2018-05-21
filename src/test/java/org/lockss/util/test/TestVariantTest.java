package org.lockss.util.test;

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
