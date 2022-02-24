/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.core;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.*;
import org.apache.commons.collections4.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.tuple.*;
import org.apache.http.*;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.EnumSource;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.PreOrderComparator;
import org.lockss.util.test.*;
import org.lockss.util.time.TimeBase;
import org.springframework.http.HttpHeaders;



// TODO:
//
// - test default methods in LockssRepository interface
// - multi-threaded (for local? & rest)
// - more realistic workflows (retrievals more interleaved with stores)
// - different headers
// - test persistence (shut down repo, recreate)

/** Test harness for LockssRepository implementations */
public abstract class AbstractLockssRepositoryTest extends LockssTestCase5 {

  /** Concrete subclasses must implement to create an instance of the
   * appropriate repository type */
  public abstract LockssRepository makeLockssRepository() throws Exception;

  private final static L4JLogger log = L4JLogger.getLogger();

  static boolean AVOID_STREAM_CLOSED_BUG = false;

  protected static int MAX_RANDOM_FILE = 50000;
  protected static int MAX_INCR_FILE = 20000;
  static {
    if (AVOID_STREAM_CLOSED_BUG) {
      // avoid Stream Closed bug by staying under 4096
      MAX_RANDOM_FILE = 4000;
      MAX_INCR_FILE = 4000;
    }
  }

  // TEST DATA

  // Commonly used artifact identifiers and contents
  protected static String COLL1 = "coll1";
  protected static String COLL2 = "coll2";
  protected static String AUID1 = "auid1";
  protected static String AUID2 = "auid2";
  protected static String ARTID1 = "art_id_1";

  protected static String URL1 = "http://host1.com/path";
  protected static String URL2 = "http://host2.com/file1";
  protected static String URL3 = "http://host2.com/file2";
  protected static String PREFIX1 = "http://host2.com/";

  protected static String CONTENT1 = "content string 1";

  protected static HttpHeaders HEADERS1 = new HttpHeaders();
  static {
    HEADERS1.set("key1", "val1");
    HEADERS1.set("key2", "val2");
  }

  protected static StatusLine STATUS_LINE_OK =
    new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");
  protected static StatusLine STATUS_LINE_MOVED =
    new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 301, "Moved");

  // Identifiers expected not to exist in the repository
  protected static String NO_COLL= "no_coll";
  protected static String NO_AUID = "no_auid";
  protected static String NO_URL = "no_url";
  protected static String NO_ARTID = "not an artifact ID";

  // Sets of coll, au, url for combinatoric tests.  Last one in each
  // differs only in case from previous, to check case-sensitivity
  protected static String[] COLLS = {COLL1, COLL2, "Coll2"};
  protected static String[] AUIDS = {AUID1, AUID2, "Auid2"};
  protected static String[] URLS = {URL1, URL2, URL2.toUpperCase()};

  // Definition of variants to run
  protected enum StdVariants {
    empty, commit1, uncommit1, url3, url3unc, disjoint,
    grid3x3x3, grid3x3x3x3,
  }

  /** Return a list of ArtifactSpecs for the initial conditions for the named
   * variant */
  public List<ArtifactSpec> getVariantSpecs(String variant) throws IOException {
    List<ArtifactSpec> res = new ArrayList<ArtifactSpec>();
    switch (variant) {
    case "no_variant":
      // Not a variant test
      break;
    case "empty":
      // Empty repository
      break;
    case "commit1":
      // One committed artifact
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      break;
    case "uncommit1":
      // One uncommitted artifact
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1));
      break;
    case "url3":
      // Three committed versions
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      break;
    case "url3unc":
      // Mix of committed and uncommitted, two URLs
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));

      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2));
      break;
    case "disjoint":
      // Different URLs in different collections and AUs
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));

      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2));
      break;
    case "overlap":
      // Same URLs in different collections and AUs
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2));
      res.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).toCommit(true));

      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).toCommit(true));
      res.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2));
      break;
    case "grid3x3x3":
      // Combinatorics of collection, AU, URL
      {
	boolean toCommit = false;
	for (String coll : COLLS) {
	  for (String auid : AUIDS) {
	    for (String url : URLS) {
	      res.add(ArtifactSpec.forCollAuUrl(coll, auid, url).toCommit(toCommit));
	      toCommit = !toCommit;
	    }
	  }
	}
      }
      break;
    case "grid3x3x3x3":
      // Combinatorics of collection, AU, URL w/ multiple versions
      {
	boolean toCommit = false;
	for (int ix = 1; ix <= 3; ix++) {
	  for (String coll : COLLS) {
	    for (String auid : AUIDS) {
	      for (String url : URLS) {
		res.add(ArtifactSpec.forCollAuUrl(coll, auid, url).toCommit(toCommit));
		toCommit = !toCommit;
	      }
	    }
	  }
	}
      }
      break;
    default:
      fail("getVariantSpecs called with unknown variant name: " + variant);
    }
    return res;
  }

  // LOCALS

  // Currently running variant name
  private String variant = "no_variant";

  protected LockssRepository repository;
  private VariantState variantState = new VariantState();

  // SETUP

  @BeforeEach
  public void beforeEach() throws Exception {
    log.debug("Running beforeEach()");
    TimeBase.setSimulated();
    setUpRepo();
    beforeVariant();
  }

  void setUpRepo() throws Exception {
    log.debug("Running setUpRepo()");
    this.repository = makeLockssRepository();
    this.repository.initRepository();
  }

  @AfterEach
  public void tearDownArtifactDataStore() throws Exception {
    log.debug("Running tearDownArtifactDataStore()");
    this.repository.shutdownRepository();
    this.repository = null;
  }

  // Set up the current variant: create appropriate ArtifactSpecs and add them
  // to the repository
  void beforeVariant() throws IOException {
    List<ArtifactSpec> scenario = getVariantSpecs(variant);
    instantiateScenario(scenario);
  }

  // Add Artifacts to the repository as specified by the ArtifactSpecs
  void instantiateScenario(List<ArtifactSpec> scenario) throws IOException {
    for (ArtifactSpec spec : scenario) {
      Artifact art = addUncommitted(spec);
      if (spec.isToCommit()) {
	commit(spec, art);
      }
    }
  }

  // Invoked automatically before each test by the @VariantTest mechanism
  @Override
  protected void setUpVariant(String variantName) {
    log.info("setUpVariant: " + variantName);
    variant = variantName;
  }

  // TESTS

  // write artifacts of increasing size, catch size-related bugs early
  @Test
  public void testArtifactSizes() throws IOException {
    for (int size = 0; size < MAX_INCR_FILE; size += 100) {
      testArtifactSize(size);
    }
  }

  public void testArtifactSize(int size) throws IOException {
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1 + size)
      .toCommit(true).setContentLength(size);
    Artifact newArt = addUncommitted(spec);
    Artifact commArt = commit(spec, newArt);
    spec.assertArtifact(repository, commArt);
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testAddArtifact() throws IOException {
    // Illegal arguments
    assertThrowsMatch(IllegalArgumentException.class,
		      "ArtifactData",
		      () -> {repository.addArtifact(null);});

    // Illegal ArtifactData (at least one null field)
    for (ArtifactData illAd : nullPointerArtData) {
      assertThrows(NullPointerException.class,
		   () -> {repository.addArtifact(illAd);});
    }

    // legal use of addArtifact is tested in the normal course of setting
    // up variants, and by testArtifactSizes(), but for the sake of
    // completeness ...

    ArtifactSpec spec = new ArtifactSpec().setUrl("https://mr/ed/").setContent(CONTENT1).setCollectionDate(0);
    Artifact newArt = addUncommitted(spec);
    Artifact commArt = commit(spec, newArt);
    spec.assertArtifact(repository, commArt);
  }

  // Test ArtifactSpec from InputStream generator.  (This is really more a
  // test of ArtifactSpec)
  @Test
  public void testArtifactSpecInputStream() throws IOException {
    ArtifactSpec spec = new ArtifactSpec()
      .setUrl("https://mr/ed/")
      .setContentGenerator(() ->
			   IOUtils.toInputStream("abcd",
						 Charset.defaultCharset()))
      .setCollectionDate(0);
    assertEquals(4, IOUtils.copy(spec.getInputStream(),
				 new NullOutputStream()));

    Artifact newArt = addUncommitted(spec);
    Artifact commArt = commit(spec, newArt);
    spec.assertArtifact(repository, commArt);
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifact() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifact(null, null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifact(null, AUID1, URL1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifact(COLL1, null, URL1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "url",
		      () -> {repository.getArtifact(COLL1, AUID1, null);});

    // Artifact not found
    for (ArtifactSpec spec : notFoundArtifactSpecs()) {
      log.info("s.b. notfound: " + spec);
      assertNull(getArtifact(repository, spec, false),
		 "Null or non-existent name shouldn't be found: " + spec);

      if (spec.isCommitted()) {
	assertNotNull(getArtifact(repository, spec, true),
		      "Null or non-existent name shouldn't be found: " + spec);
      } else {
	assertNull(getArtifact(repository, spec, false),
		   "Uncommitted shouldn't be found: " + spec);
      }
    }

    // Ensure that a no-version retrieval gets the expected highest version
    for (ArtifactSpec highSpec : variantState.getHighestCommittedVerSpecs()) {
      log.info("highSpec: " + highSpec);
      highSpec.assertArtifact(repository, repository.getArtifact(
	  highSpec.getCollection(),
	  highSpec.getAuid(),
	  highSpec.getUrl()));
    }

  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactFromId() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null",
		      () -> {repository.getArtifactFromId(null);});

    // Artifact not found
    assertNull(repository.getArtifactFromId("Not a likely artifact id"),
	       "Non-existent artifactId shouldn't be found");

    for (ArtifactSpec highSpec : variantState.getHighestCommittedVerSpecs()) {
      log.info("highSpec: " + highSpec);
      Artifact art = repository.getArtifact(highSpec.getCollection(),
					    highSpec.getAuid(),
					    highSpec.getUrl());

      assertTrue(art.equalsExceptStorageUrl(repository.getArtifactFromId(art.getId())));
    }

  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactData() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null",
		      () -> {repository.getArtifactData((String)null, (String)null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null",
		      () -> {repository.getArtifactData(null, ARTID1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null",
		      () -> {repository.getArtifactData(COLL1, null);});

    // Artifact not found
    // XXX should this throw?
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
		      "Non-existent artifact",
		      () -> {repository.getArtifactData(COLL1, NO_ARTID);});

    ArtifactSpec cspec = variantState.anyCommittedSpec();
    if (cspec != null) {
      ArtifactData ad = repository.getArtifactData(cspec.getCollection(),
						   cspec.getArtifactId());
      cspec.assertArtifactData(ad);
      // should be in TestArtifactData
      assertFalse(ad.hasContentInputStream());
      assertThrowsMatch(IllegalStateException.class,
			"Attempt to get InputStream from ArtifactData whose InputStream has been used",
			() -> ad.getInputStream());
    }
    ArtifactSpec uspec = variantState.anyUncommittedSpec();
    if (uspec != null) {
      ArtifactData ad = repository.getArtifactData(uspec.getCollection(),
						   uspec.getArtifactId());
      uspec.assertArtifactData(ad);
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactVersion() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactVersion(null, null, null, null, false);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactVersion(null, null, null, null, true);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactVersion(null, AUID1, URL1, 1, false);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactVersion(null, AUID1, URL1, 1, true);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactVersion(COLL1, null, URL1, 1, false);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactVersion(COLL1, null, URL1, 1, true);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "url",
		      () -> {repository.getArtifactVersion(COLL1, AUID1, null, 1, false);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "url",
		      () -> {repository.getArtifactVersion(COLL1, AUID1, null, 1, true);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "version",
		      () -> {repository.getArtifactVersion(COLL1, AUID1, URL1, null, false);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "version",
		      () -> {repository.getArtifactVersion(COLL1, AUID1, URL1, null, true);});
    // XXXAPI illegal version numbers
//     assertThrowsMatch(IllegalArgumentException.class,
// 		      "version",
// 		      () -> {repository.getArtifactVersion(COLL1, AUID1, URL1, -1);});
//     assertThrowsMatch(IllegalArgumentException.class,
// 		      "version",
// 		      () -> {repository.getArtifactVersion(COLL1, AUID1, URL1, 0);});

    // Artifact not found

    // notFoundArtifactSpecs() includes some that would be found with a
    // different version so can't use that here.

    for (ArtifactSpec spec : neverFoundArtifactSpecs) {
      log.info("s.b. notfound: " + spec);
      assertNull(getArtifactVersion(repository, spec, 1, false),
		 "Null or non-existent name shouldn't be found: " + spec);
      assertNull(getArtifactVersion(repository, spec, 1, true),
		 "Null or non-existent name shouldn't be found: " + spec);
      assertNull(getArtifactVersion(repository, spec, 2, false),
		 "Null or non-existent name shouldn't be found: " + spec);
      assertNull(getArtifactVersion(repository, spec, 2, true),
		 "Null or non-existent name shouldn't be found: " + spec);
    }

    // Get all added artifacts, check correctness
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      spec.assertArtifact(repository, getArtifact(repository, spec, true));

      if (spec.isCommitted()) {
	log.info("s.b. data: " + spec);
	spec.assertArtifact(repository, getArtifact(repository, spec, false));
      } else {
	log.info("s.b. uncommitted: " + spec);
	assertNull(getArtifact(repository, spec, false),
		   "Uncommitted shouldn't be found: " + spec);
      }
      // XXXAPI illegal version numbers
      assertNull(getArtifactVersion(repository, spec, 0, false));
      assertNull(getArtifactVersion(repository, spec, 0, true));
      assertNull(getArtifactVersion(repository, spec, -1, false));
      assertNull(getArtifactVersion(repository, spec, -1, true));
    }

    // Ensure that a non-existent version isn't found
    for (ArtifactSpec highSpec : variantState.getHighestVerSpecs()) {
      log.info("highSpec: " + highSpec);
      assertNull(repository.getArtifactVersion(highSpec.getCollection(),
					       highSpec.getAuid(),
					       highSpec.getUrl(),
					       highSpec.getVersion() + 1,
					       false));
      assertNull(repository.getArtifactVersion(highSpec.getCollection(),
					       highSpec.getAuid(),
					       highSpec.getUrl(),
					       highSpec.getVersion() + 1,
					       true));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testAuSize() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.auSize(null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.auSize(null, AUID1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.auSize(COLL1, null);});

    // Test expected AU size for non-existent AU
    AuSize auSize1 = repository.auSize(COLL1, NO_AUID);

    assertNotNull(auSize1);
    assertEquals(0, auSize1.getTotalAllVersions());
    assertEquals(0, auSize1.getTotalLatestVersions());
    assertEquals(-1L, auSize1.getTotalWarcSize()); // TODO

    // Calculate the expected size of each AU in each collection, compare with auSize()
    for (String coll : variantState.activeCollections()) {
      for (String auid : variantState.allAuids()) {
        long expTotalAllVersions = variantState.committedSpecStream()
            .filter(spec -> spec.getAuid().equals(auid))
            .filter(spec -> spec.getCollection().equals(coll))
            .mapToLong(ArtifactSpec::getContentLength)
            .sum();

        long expTotalLatestVersions = variantState.getHighestCommittedVerSpecs().stream()
            .filter(spec -> spec.getAuid().equals(auid))
            .filter(spec -> spec.getCollection().equals(coll))
            .mapToLong(ArtifactSpec::getContentLength)
            .sum();

        AuSize auSize = repository.auSize(coll, auid);

        assertEquals((long) expTotalAllVersions, (long) auSize.getTotalAllVersions());
        assertEquals((long) expTotalLatestVersions, (long) auSize.getTotalLatestVersions());
        assertEquals(-1L, (long) auSize.getTotalWarcSize()); // TODO
      }
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testCommitArtifact() throws IOException {
    // Illegal args
    assertThrows(IllegalArgumentException.class,
		 () -> {repository.commitArtifact(null, null);});
    assertThrows(IllegalArgumentException.class,
		 () -> {repository.commitArtifact(null, ARTID1);});
    assertThrows(IllegalArgumentException.class,
		 () -> {repository.commitArtifact(COLL1, null);});

    // Commit already committed artifact
    ArtifactSpec commSpec = variantState.anyCommittedSpec();
    if (commSpec != null) {
      // Get the existing artifact
      Artifact commArt = getArtifact(repository, commSpec, false);
      // XXXAPI should this throw?
//       assertThrows(NullPointerException.class,
// 		   () -> {repository.commitArtifact(commSpec.getCollection(),
// 						    commSpec.getArtifactId());});
      Artifact dupArt = repository.commitArtifact(commSpec.getCollection(),
						  commSpec.getArtifactId());
      assertTrue(commArt.equalsExceptStorageUrl(dupArt));
      commSpec.assertArtifact(repository, dupArt);

      // Get the same artifact when uncommitted may be included.
      commArt = getArtifact(repository, commSpec, true);
      commSpec.assertArtifact(repository, commArt);
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testDeleteArtifact() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or artifact id",
		      () -> {repository.deleteArtifact(null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "artifact",
		      () -> {repository.deleteArtifact(COLL1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.deleteArtifact(null, AUID1);});

    // Delete non-existent artifact
    // XXXAPI
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
		      "Non-existent artifact id: " + NO_ARTID,
		      () -> {repository.deleteArtifact(NO_COLL, NO_ARTID);});

    {
      // Delete a committed artifact that isn't the highest version. it
      // should disappear but size shouldn't change
      ArtifactSpec spec = variantState.committedSpecStream()
	.filter(s -> s != variantState.getHighestCommittedVerSpec(s.artButVerKey()))
	.findAny().orElse(null);
      if (spec != null) {

  AuSize auSize1 = repository.auSize(spec.getCollection(), spec.getAuid());

  assertNotNull(repository.getArtifactData(spec.getCollection(), spec.getArtifactId()));
	assertNotNull(getArtifact(repository, spec, false));
	assertNotNull(getArtifact(repository, spec, true));
	log.info("Deleting not highest: " + spec);
	repository.deleteArtifact(spec.getCollection(), spec.getArtifactId());

        assertThrows(
            LockssNoSuchArtifactIdException.class,
            () -> repository.getArtifactData(spec.getCollection(), spec.getArtifactId())
        );

  assertNull(getArtifact(repository, spec, false));
	assertNull(getArtifact(repository, spec, true));
	variantState.delFromAll(spec);

  AuSize auSize2  = repository.auSize(spec.getCollection(), spec.getAuid());

  // Assert totalLatestVersions remains the same but totalAllVersions is different
  assertEquals(auSize1.getTotalLatestVersions(), auSize2.getTotalLatestVersions(),
      "Latest versions size changed after deleting non-highest version");
  assertNotEquals( auSize1.getTotalAllVersions(), auSize2.getTotalAllVersions(),
      "All versions size did NOT change after deleting non-highest version");
  assertEquals(auSize1.getTotalWarcSize(), auSize2.getTotalWarcSize());
      }
    }

    {
      // Delete a highest-version committed artifact, it should disappear and
      // size should change
      ArtifactSpec spec = variantState.getHighestCommittedVerSpecs().stream()
	.findAny().orElse(null);
      if (spec != null) {
  AuSize auSize1 = repository.auSize(spec.getCollection(), spec.getAuid());

	long artsize = spec.getContentLength();
  assertNotNull(repository.getArtifactData(spec.getCollection(), spec.getArtifactId()));
	assertNotNull(getArtifact(repository, spec, false));
	assertNotNull(getArtifact(repository, spec, true));
	log.info("Deleting highest: " + spec);
	repository.deleteArtifact(spec.getCollection(), spec.getArtifactId());

        assertThrows(
            LockssNoSuchArtifactIdException.class,
            () -> repository.getArtifactData(spec.getCollection(), spec.getArtifactId())
        );

	assertNull(getArtifact(repository, spec, false));
	assertNull(getArtifact(repository, spec, true));
	variantState.delFromAll(spec);

  long expectedTotalAllVersions = auSize1.getTotalAllVersions() - artsize;
  long expectedTotalLatestVersions = auSize1.getTotalLatestVersions() - artsize;

	ArtifactSpec newHigh = variantState.getHighestCommittedVerSpec(spec.artButVerKey());
	if (newHigh != null) {
	  expectedTotalLatestVersions += newHigh.getContentLength();
	}

  AuSize auSize2 = repository.auSize(spec.getCollection(), spec.getAuid());

  assertEquals(expectedTotalAllVersions, auSize2.getTotalAllVersions(),
      variant + ": AU all artifact versions size wrong after deleting highest version");
	assertEquals(expectedTotalLatestVersions, auSize2.getTotalLatestVersions(),
		     variant + ": AU latest artifact versions size wrong after deleting highest version");
  assertEquals(-1L, auSize2.getTotalWarcSize(), // TODO
      variant + ": AU latest artifact versions size wrong after deleting highest version");
      }
    }

    // Delete an uncommitted artifact, it should disappear and size should
    // not change
    {
      ArtifactSpec uspec = variantState.anyUncommittedSpec();
      if (uspec != null) {
  AuSize auSize1 = repository.auSize(uspec.getCollection(), uspec.getAuid());

  assertNotNull(repository.getArtifactData(uspec.getCollection(), uspec.getArtifactId()));
	assertNull(getArtifact(repository, uspec, false));
	assertNotNull(getArtifact(repository, uspec, true));
	log.info("Deleting uncommitted: " + uspec);
	repository.deleteArtifact(uspec.getCollection(), uspec.getArtifactId());

        assertThrows(
            LockssNoSuchArtifactIdException.class,
            () -> repository.getArtifactData(uspec.getCollection(), uspec.getArtifactId())
        );

	assertNull(getArtifact(repository, uspec, false));
	assertNull(getArtifact(repository, uspec, true));
	variantState.delFromAll(uspec);

  AuSize auSize2 = repository.auSize(uspec.getCollection(), uspec.getAuid());

  assertEquals(auSize1, auSize2, "AU size changed after deleting uncommitted");
      }
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testDeleteAllArtifacts() throws IOException {
    // TK Delete committed & uncommitted arts & check results each time
    // delete twice
    // check getAuIds() & getCollectionIds() as they run out
    Iterator<String> collIter = repository.getCollectionIds().iterator();

    // Loop through all the artifacts.
    Iterator<ArtifactSpec> iter =
	(Iterator<ArtifactSpec>)variantState.getArtifactSpecs().iterator();

    while (iter.hasNext()) {
      // Get the next artifact.
      ArtifactSpec spec = iter.next();
      String coll = spec.getCollection();
      String id = spec.getArtifactId();
      assertNotNull(repository.getArtifactData(coll, id));
      // Delete the artifact.
      repository.deleteArtifact(coll, id);

      assertThrows(
          LockssNoSuchArtifactIdException.class,
          () -> repository.getArtifactData(coll, id)
      );

      // Delete it again.
      try {
	repository.deleteArtifact(coll, id);
	fail("Should have thrown LockssNoSuchArtifactIdException");
      } catch (LockssNoSuchArtifactIdException iae) {}

      assertThrows(
          LockssNoSuchArtifactIdException.class,
          () -> repository.getArtifactData(coll, id)
      );

    }

    // There are no collections now.
    assertFalse(repository.getCollectionIds().iterator().hasNext());

    // There are no AUIds in any of the previously existing collections.
    while (collIter.hasNext()) {
      assertFalse(repository.getAuIds(collIter.next()).iterator().hasNext());
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifacts() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or au id",
		      () -> {repository.getArtifacts(null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifacts(COLL1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifacts(null, AUID1);});

    // Non-existent collection & auid
    assertEmpty(repository.getArtifacts(NO_COLL, NO_AUID));

    String anyColl = null;
    String anyAuid = null;

    // Compare with all URLs in each AU
    for (String coll : variantState.activeCollections()) {
      anyColl = coll;
      for (String auid : variantState.allAuids()) {
	anyAuid = auid;
	ArtifactSpec.assertArtList(repository, (variantState.orderedAllAu(coll, auid)
		       .filter(distinctByKey(ArtifactSpec::artButVerKey))),
		      repository.getArtifacts(coll, auid));

      }
    }

    // Combination of coll and au id that both exist, but have no artifacts
    // in common
    Pair<String,String> collau = collAuMismatch();
    if (collau != null) {
      assertEmpty(repository.getArtifacts(collau.getLeft(),
					  collau.getRight()));
    }
    // non-existent coll, au
    if (anyColl != null && anyAuid != null) {
      assertEmpty(repository.getArtifacts(anyColl,
					  anyAuid + "_notAuSuffix"));
      assertEmpty(repository.getArtifacts(anyColl + "_notCollSuffix",
					  anyAuid));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactsWithPrefix() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id, au id or prefix",
		      () -> {repository.getArtifactsWithPrefix(null, null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "prefix",
		      () -> {repository.getArtifactsWithPrefix(COLL1, AUID1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactsWithPrefix(COLL1, null, PREFIX1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactsWithPrefix(null, AUID1, PREFIX1);});

    // Non-existent collection & auid
    assertEmpty(repository.getArtifactsWithPrefix(NO_COLL, NO_AUID, PREFIX1));
    // Compare with all URLs matching prefix in each AU
    for (String coll : variantState.activeCollections()) {
      for (String auid : variantState.allAuids()) {
	ArtifactSpec.assertArtList(repository, (variantState.orderedAllAu(coll, auid)
		       .filter(spec -> spec.getUrl().startsWith(PREFIX1))
		       .filter(distinctByKey(ArtifactSpec::artButVerKey))),
		       repository.getArtifactsWithPrefix(coll, auid, PREFIX1));
	assertEmpty(repository.getArtifactsWithPrefix(coll, auid,
						      PREFIX1 + "notpath"));
      }
    }

    // Combination of coll and au id that both exist, but have no artifacts
    // in common
    Pair<String,String> collau = collAuMismatch();
    if (collau != null) {
      assertEmpty(repository.getArtifactsWithPrefix(collau.getLeft(),
						    collau.getRight(),
						    PREFIX1));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactsAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or au id",
		      () -> {repository.getArtifactsAllVersions(null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactsAllVersions(COLL1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactsAllVersions(null, AUID1);});

    // Non-existent collection & auid
    assertEmpty(repository.getArtifactsAllVersions(NO_COLL, NO_AUID));

    String anyColl = null;
    String anyAuid = null;
    // Compare with all URLs all version in each AU
    for (String coll : variantState.activeCollections()) {
      anyColl = coll;
      for (String auid : variantState.allAuids()) {
	anyAuid = auid;
	ArtifactSpec.assertArtList(repository, variantState.orderedAllAu(coll, auid),
		      repository.getArtifactsAllVersions(coll, auid));

      }
    }
    // Combination of coll and au id that both exist, but have no artifacts
    // in common
    Pair<String,String> collau = collAuMismatch();
    if (collau != null) {
      assertEmpty(repository.getArtifactsAllVersions(collau.getLeft(),
							collau.getRight()));
    }
    if (anyColl != null && anyAuid != null) {
      assertEmpty(repository.getArtifactsAllVersions(anyColl,
						     anyAuid + "_not"));
      assertEmpty(repository.getArtifactsAllVersions(anyColl + "_not",
						     anyAuid));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactsWithPrefixAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id, au id or prefix",
		      () -> {repository.getArtifactsWithPrefixAllVersions(null, null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "prefix",
		      () -> {repository.getArtifactsWithPrefixAllVersions(COLL1, AUID1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactsWithPrefixAllVersions(COLL1, null, PREFIX1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactsWithPrefixAllVersions(null, AUID1, PREFIX1);});

    // Non-existent collection & auid
    assertEmpty(repository.getArtifactsWithPrefixAllVersions(NO_COLL, NO_AUID, PREFIX1));
    // Compare with all URLs matching prefix in each AU
    for (String coll : variantState.activeCollections()) {
      for (String auid : variantState.allAuids()) {
	ArtifactSpec.assertArtList(repository, (variantState.orderedAllAu(coll, auid)
		       .filter(spec -> spec.getUrl().startsWith(PREFIX1))),
		       repository.getArtifactsWithPrefixAllVersions(coll, auid, PREFIX1));
	assertEmpty(repository.getArtifactsWithPrefixAllVersions(coll, auid,
								 PREFIX1 + "notpath"));
      }
    }

    // Combination of coll and au id that both exist, but have no artifacts
    // in common
    Pair<String,String> collau = collAuMismatch();
    if (collau != null) {
      assertEmpty(repository.getArtifactsWithPrefixAllVersions(collau.getLeft(),
							       collau.getRight(),
							       PREFIX1));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactsWithUrlPrefixFromAllAus_allVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or prefix",
		      () -> {repository.getArtifactsWithUrlPrefixFromAllAus(null, null, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "prefix",
		      () -> {repository.getArtifactsWithUrlPrefixFromAllAus(COLL1, null, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.getArtifactsWithUrlPrefixFromAllAus(null, PREFIX1, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
        "Versions must be ALL or LATEST",
        () -> {repository.getArtifactsWithUrlPrefixFromAllAus(COLL1, PREFIX1, null);});

    // Non-existent collection
    assertEmpty(repository.getArtifactsWithUrlPrefixFromAllAus(NO_COLL, PREFIX1, ArtifactVersions.ALL));

    // Compare with all URLs matching prefix
    for (String coll : variantState.activeCollections()) {
      ArtifactSpec.assertArtList(repository, (variantState.orderedAllCollAllAus(coll)
		  .filter(spec -> spec.getUrl().startsWith(PREFIX1))),
		  repository.getArtifactsWithUrlPrefixFromAllAus(coll, PREFIX1, ArtifactVersions.ALL));
      assertEmpty(repository.getArtifactsWithUrlPrefixFromAllAus(coll,
								     PREFIX1 + "notpath", ArtifactVersions.ALL));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactsWithUrlPrefixFromAllAus_latestVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null collection id or prefix",
        () -> {repository.getArtifactsWithUrlPrefixFromAllAus(null, null, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "prefix",
        () -> {repository.getArtifactsWithUrlPrefixFromAllAus(COLL1, null, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "collection",
        () -> {repository.getArtifactsWithUrlPrefixFromAllAus(null, PREFIX1, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "Versions must be ALL or LATEST",
        () -> {repository.getArtifactsWithUrlPrefixFromAllAus(COLL1, PREFIX1, null);});

    // Non-existent collection
    assertEmpty(repository.getArtifactsWithUrlPrefixFromAllAus(NO_COLL, PREFIX1, ArtifactVersions.LATEST));

    // Compare with all URLs matching prefix
    for (String collection : variantState.activeCollections()) {
      ArtifactSpec.assertArtList(
          repository,
          /* Expected */
          (variantState.orderedAllCollAllAus(collection)
              .filter(spec -> spec.getUrl().startsWith(PREFIX1))
              .collect(Collectors.groupingBy(
                  spec -> new ArtifactIdentifier.ArtifactStem(spec.getCollection(), spec.getAuid(), spec.getUrl()),
                  Collectors.maxBy(Comparator.comparingInt(ArtifactSpec::getVersion))))
              .values()
              .stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .sorted(
                  // ArtifactSpec equivalent of ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION
                  Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
                      .thenComparing(ArtifactSpec::getAuid)
                      .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed())
              )
          ),
          /* Actual */
          repository.getArtifactsWithUrlPrefixFromAllAus(collection, PREFIX1, ArtifactVersions.LATEST));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id, au id or url",
		      () -> {repository.getArtifactsAllVersions(null, null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "url",
		      () -> {repository.getArtifactsAllVersions(COLL1, AUID1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "au",
		      () -> {repository.getArtifactsAllVersions(COLL1, null, URL1);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "coll",
		      () -> {repository.getArtifactsAllVersions(null, AUID1, URL1);});

    // Non-existent collection, auid or url
    assertEmpty(repository.getArtifactsAllVersions(NO_COLL, AUID1, URL1));
    assertEmpty(repository.getArtifactsAllVersions(COLL1, NO_AUID, URL1));
    assertEmpty(repository.getArtifactsAllVersions(COLL1, AUID1, NO_URL));

    // For each ArtButVer in the repository, enumerate all its versions and
    // compare with expected
    Stream<ArtifactSpec> s =
      variantState.committedSpecStream().filter(distinctByKey(ArtifactSpec::artButVerKey));
    for (ArtifactSpec urlSpec : (Iterable<ArtifactSpec>)s::iterator) {
      ArtifactSpec.assertArtList(repository, variantState.orderedAllCommitted()
		    .filter(spec -> spec.sameArtButVer(urlSpec)),
		    repository.getArtifactsAllVersions(urlSpec.getCollection(),
						       urlSpec.getAuid(),
						       urlSpec.getUrl()));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactAllVersionsAllAus_allVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or url",
		      () -> {repository.getArtifactsWithUrlFromAllAus(null, null, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "url",
		      () -> {repository.getArtifactsWithUrlFromAllAus(COLL1, null, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "coll",
		      () -> {repository.getArtifactsWithUrlFromAllAus(null, URL1, ArtifactVersions.ALL);});
    assertThrowsMatch(IllegalArgumentException.class,
        "Versions must be ALL or LATEST",
        () -> {repository.getArtifactsWithUrlFromAllAus(COLL1, URL1, null);});

    // Non-existent collection or url
    assertEmpty(repository.getArtifactsWithUrlFromAllAus(NO_COLL, URL1, ArtifactVersions.ALL));
    assertEmpty(repository.getArtifactsWithUrlFromAllAus(COLL1, NO_URL, ArtifactVersions.ALL));

    // For each ArtButVer in the repository, enumerate all its versions and
    // compare with expected
    Stream<ArtifactSpec> s =
      variantState.committedSpecStream().filter(distinctByKey(ArtifactSpec::artButVerKey));
    for (ArtifactSpec urlSpec : (Iterable<ArtifactSpec>)s::iterator) {
      ArtifactSpec.assertArtList(repository, variantState.orderedAllCommittedAllAus()
		    .filter(spec -> spec.sameArtButVerAllAus(urlSpec)),
		    repository.getArtifactsWithUrlFromAllAus(urlSpec.getCollection(),
						             urlSpec.getUrl(), ArtifactVersions.ALL));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetArtifactAllVersionsAllAus_latestVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null collection id or url",
        () -> {repository.getArtifactsWithUrlFromAllAus(null, null, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "url",
        () -> {repository.getArtifactsWithUrlFromAllAus(COLL1, null, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "coll",
        () -> {repository.getArtifactsWithUrlFromAllAus(null, URL1, ArtifactVersions.LATEST);});
    assertThrowsMatch(IllegalArgumentException.class,
        "Versions must be ALL or LATEST",
        () -> {repository.getArtifactsWithUrlFromAllAus(COLL1, URL1, null);});

    // Non-existent collection or url
    assertEmpty(repository.getArtifactsWithUrlFromAllAus(NO_COLL, URL1, ArtifactVersions.LATEST));
    assertEmpty(repository.getArtifactsWithUrlFromAllAus(COLL1, NO_URL, ArtifactVersions.LATEST));

    // Compare with all URLs matching prefix
    // For each ArtButVer in the repository, enumerate all its versions and
    // compare with expected
    Stream<ArtifactSpec> s =
        variantState.committedSpecStream().filter(distinctByKey(ArtifactSpec::artButVerKey));
    for (ArtifactSpec urlSpec : (Iterable<ArtifactSpec>)s::iterator) {
      ArtifactSpec.assertArtList(
          repository,
          /* Expected */
          (variantState.orderedAllCommittedAllAus()
              .filter(spec -> spec.sameArtButVerAllAus(urlSpec))
              .collect(Collectors.groupingBy(
                  spec -> new ArtifactIdentifier.ArtifactStem(spec.getCollection(), spec.getAuid(), spec.getUrl()),
                  Collectors.maxBy(Comparator.comparingInt(ArtifactSpec::getVersion))))
              .values()
              .stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .sorted(
                  // ArtifactSpec equivalent of ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION
                  Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
                      .thenComparing(ArtifactSpec::getAuid)
                      .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed())
              )
          ),
          /* Actual */
          repository.getArtifactsWithUrlFromAllAus(urlSpec.getCollection(), urlSpec.getUrl(), ArtifactVersions.LATEST));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetAuIds() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection",
		      () -> {repository.getAuIds(null);});

    // Non-existent collection
    assertEmpty(repository.getAuIds(NO_COLL));

    // Compare with expected auid list for each collection
    for (String coll : variantState.activeCollections()) {
      Iterator<String> expAuids =
	variantState.orderedAllCollIncludeUncommitted(coll)
	.map(ArtifactSpec::getAuid)
	.distinct()
	.iterator();
      assertEquals(IteratorUtils.toList(expAuids),
		   IteratorUtils.toList(repository.getAuIds(coll).iterator()));
    }
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testGetCollectionIds() throws IOException {
    Iterator<String> expColl =
      variantState.orderedAll()
      .map(ArtifactSpec::getCollection)
      .distinct()
      .iterator();
      assertEquals(IteratorUtils.toList(expColl),
		   IteratorUtils.toList(repository.getCollectionIds().iterator()));
  }

  @VariantTest
  @EnumSource(StdVariants.class)
  public void testIsArtifactCommitted() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
		      "Null collection id or artifact id",
		      () -> {repository.isArtifactCommitted(null, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "artifact",
		      () -> {repository.isArtifactCommitted(COLL1, null);});
    assertThrowsMatch(IllegalArgumentException.class,
		      "collection",
		      () -> {repository.isArtifactCommitted(null, ARTID1);});

    // non-existent collection, artifact id

    // XXXAPI
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
		      "Non-existent artifact id: " + NO_ARTID,
		      () -> {repository.isArtifactCommitted(COLL1, NO_ARTID);});
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
		      "Non-existent artifact id: " + ARTID1,
		      () -> {repository.isArtifactCommitted(NO_COLL, ARTID1);});

//     assertFalse(repository.isArtifactCommitted(COLL1, NO_ARTID));
//     assertFalse(repository.isArtifactCommitted(NO_COLL, ARTID1));

    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isCommitted()) {
	assertTrue(repository.isArtifactCommitted(spec.getCollection(),
						  spec.getArtifactId()));
      } else {
	assertFalse(repository.isArtifactCommitted(spec.getCollection(),
						   spec.getArtifactId()));
      }
    }

  }

  // UTILITIES

  public static <T> Predicate<T> distinctByKey(Function<? super T,Object> keyExtractor) {
    Set<Object> seen = new HashSet<>();
    return t -> seen.add(keyExtractor.apply(t));
  }

  // Find a collection and an au that each have artifacts, but don't have
  // any artifacts in common
  Pair<String,String> collAuMismatch() {
    Set<Pair<String,String>> set = new HashSet<Pair<String,String>>();
    for (String coll : variantState.activeCommittedCollections()) {
      for (String auid : variantState.addedCommittedAuids()) {
	set.add(new ImmutablePair<String, String>(coll, auid));
      }
    }
    variantState.committedSpecStream()
      .forEach(spec -> {set.remove(
	  new ImmutablePair<String, String>(spec.getCollection(),
					    spec.getAuid()));});
    if (set.isEmpty()) {
      return null;
    } else {
      Pair<String,String> res = set.iterator().next();
      log.info("Found coll au mismatch: " +
	       res.getLeft() + ", " + res.getRight());
      variantState.logAdded();
      return res;
    }
  }

  Artifact getArtifact(LockssRepository repository, ArtifactSpec spec,
      boolean includeUncommitted) throws IOException {
    log.info(String.format("getArtifact(%s, %s, %s, %s)",
			   spec.getCollection(),
			   spec.getAuid(),
			   spec.getUrl(),
			   includeUncommitted));
    if (spec.hasVersion()) {
      return repository.getArtifactVersion(spec.getCollection(),
					   spec.getAuid(),
					   spec.getUrl(),
					   spec.getVersion(),
					   includeUncommitted);
    } else {
      return repository.getArtifact(spec.getCollection(),
				    spec.getAuid(),
				    spec.getUrl());
    }
  }

  Artifact getArtifactVersion(LockssRepository repository, ArtifactSpec spec,
			      int ver, boolean includeUncommitted)
      throws IOException {
    log.info(String.format("getArtifactVersion(%s, %s, %s, %d, %s)",
			   spec.getCollection(),
			   spec.getAuid(),
			   spec.getUrl(),
			   ver,
			   includeUncommitted));
    return repository.getArtifactVersion(spec.getCollection(),
					 spec.getAuid(),
					 spec.getUrl(),
					 ver, includeUncommitted);
  }

  Artifact addUncommitted(ArtifactSpec spec) throws IOException {
    if (!spec.hasContent()) {
      spec.generateContent();
    }
    log.info("adding: " + spec);

    ArtifactData ad = spec.getArtifactData();
    Artifact newArt = repository.addArtifact(ad);
    assertNotNull(newArt);

    try {
      spec.assertArtifact(repository, newArt);
    } catch (Exception e) {
      log.error("Caught exception adding uncommitted artifact: {}", e);
      log.error("spec = {}", spec);
      log.error("ad = {}", ad);
      log.error("newArt = {}", newArt);
      throw e;
    }
    long expVers = variantState.expectedVersions(spec);
    assertEquals(expVers + 1, (int)newArt.getVersion(),
		 "version of " + newArt);
    if (spec.getExpVer() >= 0) {
      throw new IllegalStateException("addUncommitted() must be called with unused ArtifactSpec");
    }

    String newArtId = newArt.getId();
    assertNotNull(newArtId);
    assertFalse(repository.isArtifactCommitted(spec.getCollection(),
					       newArtId));
    assertFalse(newArt.getCommitted());

    try (ArtifactData artifact = repository.getArtifactData(spec.getCollection(), newArtId)) {
      assertNotNull(artifact);
    }

    Artifact oldArt = getArtifact(repository, spec, false);
    if (expVers == 0) {
      // this test valid only when no other versions exist ArtifactSpec
      assertNull(oldArt);
    }

    if (spec.hasVersion()) {
      assertEquals(newArt, getArtifact(repository, spec, true));
    }

    spec.setVersion(newArt.getVersion());
    spec.setArtifactId(newArtId);

    variantState.add(spec);

    return newArt;
  }

  Artifact commit(ArtifactSpec spec, Artifact art) throws IOException {
    Artifact uncommittedArt = getArtifact(repository, spec, true);
    assertNotNull(uncommittedArt);
    assertFalse(repository.isArtifactCommitted(spec.getCollection(),
					       uncommittedArt.getId()));
    assertFalse(uncommittedArt.getCommitted());

    String artId = art.getId();
    log.info("committing: " + art);
    Artifact commArt = null;
    try {
      commArt = repository.commitArtifact(spec.getCollection(), artId);
      variantState.commit(spec.getArtifactId());
    } catch (Exception e) {
      log.error("Caught exception committing artifact: {}", e);
      log.error("spec = {}", spec);
      log.error("art = {}", art);
      log.error("artId = {}", artId);
      throw e;
    }
    assertNotNull(commArt);
    if (spec.getExpVer() > 0) {
      assertEquals(spec.getExpVer(), (int)commArt.getVersion());
    }

    spec.setCommitted(true);

    assertTrue(repository.isArtifactCommitted(spec.getCollection(),
					      commArt.getId()));
    assertTrue(commArt.getCommitted());

    spec.assertArtifact(repository, commArt);

    Artifact newArt = getArtifact(repository, spec, false);
    assertNotNull(newArt);
    assertTrue(repository.isArtifactCommitted(spec.getCollection(),
					      newArt.getId()));
    assertTrue(newArt.getCommitted());
    assertNotNull(repository.getArtifactData(spec.getCollection(), newArt.getId()));
    // Get the same artifact when uncommitted may be included.
    newArt = getArtifact(repository, spec, true);
    assertNotNull(newArt);
    return newArt;
  }

  InputStream stringInputStream(String str) {
    return IOUtils.toInputStream(str, Charset.defaultCharset());
  }

  // These should all cause addArtifact to throw NPE
  protected ArtifactData[] nullPointerArtData = {
    new ArtifactData(null, null, null),
    new ArtifactData(null, null, STATUS_LINE_OK),
    new ArtifactData(null, stringInputStream(""), null),
    new ArtifactData(null, stringInputStream(""), STATUS_LINE_OK),
    new ArtifactData(HEADERS1, null, null),
    new ArtifactData(HEADERS1, null, STATUS_LINE_OK),
    new ArtifactData(HEADERS1, stringInputStream(""), null),
  };

  // These describe artifacts that getArtifact() should never find
  protected ArtifactSpec[] neverFoundArtifactSpecs = {
    ArtifactSpec.forCollAuUrl(NO_COLL, AUID1, URL1),
    ArtifactSpec.forCollAuUrl(COLL1, NO_AUID, URL1),
    ArtifactSpec.forCollAuUrl(COLL1, AUID1, NO_URL),
  };

  /** Return list of ArtifactSpecs that shouldn't be found in the current
   * repository */
  protected List<ArtifactSpec> notFoundArtifactSpecs() {
    List<ArtifactSpec> res = new ArrayList<ArtifactSpec>();
    // Always include some that should never be found
    Collections.addAll(res, neverFoundArtifactSpecs);

    // Include an uncommitted artifact, if any
    ArtifactSpec uncSpec = variantState.anyUncommittedSpecButVer();
    if (uncSpec != null) {
      log.info("adding an uncommitted spec: " + uncSpec);
      res.add(uncSpec);
    }

    // If there's at least one committed artifact ...
    ArtifactSpec commSpec = variantState.anyCommittedSpec();
    if (commSpec != null) {
      // include variants of it with non-existent collection, au, etc.
      res.add(commSpec.copy().setCollection("NO_" + commSpec.getCollection()));
      res.add(commSpec.copy().setAuid("NO_" + commSpec.getAuid()));
      res.add(commSpec.copy().setUrl("NO_" + commSpec.getUrl()));

      // and with existing but different collection, au
      diff_coll:
      for (ArtifactSpec auUrl : variantState.committedSpecStream()
	     .filter(distinctByKey(s -> s.getUrl() + "|" + s.getAuid()))
	     .collect(Collectors.toList())) {
	for (String coll : variantState.activeCommittedCollections()) {
	  ArtifactSpec a = auUrl.copy().setCollection(coll);
	  if (!variantState.hasHighestCommittedVerSpec(a.artButVerKey())) {
	    res.add(a);
	    break diff_coll;
	  }
	}
      }
      diff_au:
      for (ArtifactSpec auUrl : variantState.committedSpecStream()
	     .filter(distinctByKey(s -> s.getUrl() + "|" + s.getCollection()))
	     .collect(Collectors.toList())) {
	for (String auid : variantState.addedCommittedAuids()) {
	  ArtifactSpec a = auUrl.copy().setAuid(auid);
	  if (!variantState.hasHighestCommittedVerSpec(a.artButVerKey())) {
	    res.add(a);
	    break diff_au;
	  }
	}
      }
      diff_url:
      for (ArtifactSpec auUrl : variantState.committedSpecStream()
	     .filter(distinctByKey(s -> s.getAuid() + "|" + s.getCollection()))
	     .collect(Collectors.toList())) {
	for (String url : variantState.addedCommittedUrls()) {
	  ArtifactSpec a = auUrl.copy().setUrl(url);
	  if (!variantState.hasHighestCommittedVerSpec(a.artButVerKey())) {
	    res.add(a);
	    break diff_url;
	  }
	}
      }

      // and with correct coll, au, url but non-existent version
      res.add(commSpec.copy().setVersion(0));
      res.add(commSpec.copy().setVersion(1000));
    }

    return res;
  }
}
