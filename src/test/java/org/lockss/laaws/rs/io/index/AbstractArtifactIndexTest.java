/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.io.index;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.ListUtil;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.test.VariantTest;
import org.lockss.util.time.Deadline;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractArtifactIndexTest<AI extends ArtifactIndex> extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  protected AI index;

  // *******************************************************************************************************************
  // * ABSTRACT METHODS
  // *******************************************************************************************************************

  protected abstract AI makeArtifactIndex() throws Exception;

  public abstract void testInitIndex() throws Exception;
  public abstract void testShutdownIndex() throws Exception;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @BeforeEach
  public void setupCommon() throws Exception {
    // Create an artifact index instance to test; initialize it
    index = makeArtifactIndex();
    index.initIndex();

    // Invoke before variant steps
    beforeVariant();
  }

  // *******************************************************************************************************************
  // * VARIANTS FRAMEWORK
  // *******************************************************************************************************************

  protected VariantState variantState = new VariantState();
  protected String variant = "no_variant";

  public enum TestIndexScenarios {
    empty, commit1, delete1, double_delete, double_commit, commit_delete_2x2
  }

  // Invoked automatically before each test by the @VariantTest mechanism
  @Override
  protected void setUpVariant(String variantName) {
    log.info("setUpVariant: " + variantName);
    variant = variantName;
  }

  protected void beforeVariant() throws Exception {
    // Get artifact specs to populate the index under test with
    List<ArtifactSpec> artifactSpecs = getArtifactSpecsForVariant(variant);

    log.debug("variant: {}, artifactSpecs.size() = {}", variant, artifactSpecs.size());

    populateIndex(index, artifactSpecs);
  }

  protected List<ArtifactSpec> getArtifactSpecsForVariant(String variant) {
    List<ArtifactSpec> specs = new ArrayList<>();

    switch (variant) {
      case "no_variant":
        // Not a variant test
        break;

      case "empty":
        // Empty data store
        break;

      case "commit1":
        // One committed artifact
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit());
        break;

      case "delete1":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete());
        break;

      case "double_delete":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete().thenDelete());
        break;

      case "double_commit":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit().thenCommit());
        break;

      case "commit_delete_2x2":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete().thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit().thenDelete());
        break;
    }

    return specs;
  }

//  protected void populateIndex(List<ArtifactSpec> artifactSpecs) throws IOException {
//    populateIndex(index, artifactSpecs);
//  }

  protected void populateIndex(AI index, List<ArtifactSpec> artifactSpecs) throws IOException {
    for (ArtifactSpec spec : artifactSpecs) {
      // Assign random artifact ID
      spec.setArtifactId(UUID.randomUUID().toString());

      // Add artifact from this ArtifactSpec into index
      indexArtifactSpec(index, spec);
    }
  }

  protected void indexArtifactSpec(ArtifactSpec spec) throws IOException {
    indexArtifactSpec(index, spec);
  }

  protected void indexArtifactSpec(AI index, ArtifactSpec spec) throws IOException {
    // Generate content if needed (Artifact objects must represent)
    if (!spec.hasContent()) {
      spec.generateContent();
      spec.setStorageUrl(URI.create(spec.getArtifactId()));
    }

    // Set version if one was not provided
    if (spec.getVersion() < 0) {
      ArtifactSpec highest = variantState.getHighestVerSpec(spec.artButVerKey());
      spec.setVersion((highest == null) ? 1 : highest.getVersion() + 1);
    }

    // Add artifact to index
    ArtifactData ad = spec.getArtifactData();
    Artifact addedArtifact = index.indexArtifact(ad);
    variantState.add(spec);
    spec.assertArtifactCommon(addedArtifact);

    // Sanity checks
    assertTrue(index.artifactExists(spec.getArtifactId()));
    Artifact indexedArtifact = index.getArtifact(spec.getArtifactId());
    assertNotNull(indexedArtifact);
    spec.assertArtifactCommon(indexedArtifact);

    // Replay operations on artifact
    for (ArtifactSpec.ArtifactDataStoreOperation op : spec.getDataStoreOperations()) {
      switch (op) {
        case COMMIT:
          index.commitArtifact(spec.getArtifactId());
          spec.setCommitted(true);
          variantState.commit(spec.getArtifactId());
          log.debug("Committed artifact from artifact specification [spec: {}]", spec);
          break;

        case DELETE:
          index.deleteArtifact(spec.getArtifactId());
          spec.setDeleted(true);
          variantState.delFromAll(spec);
          log.debug("Deleted artifact from artifact specification [spec: {}]", spec);
          break;

        default:
          log.warn("Unknown artifact operation in spec: [spec: {}, op: {}]", spec, op);
          continue;
      }
    }
  }

  // *******************************************************************************************************************
  // * TEST METHODS
  // *******************************************************************************************************************

  @Test
  public void testWaitReady() throws Exception {
    // Instantiate a new index (that has not had initIndex() called already)
    ArtifactIndex index = makeArtifactIndex();

    // Assert waiting on a deadline that expires immediately results in a TimeoutException thrown
    assertThrows(TimeoutException.class, () -> index.waitReady(Deadline.in(0L)));

    // Initialize the index in a separate thread
    new Thread(() -> index.initIndex()).start();

    // Assert waiting with a sufficient deadline works
    try {
      log.debug("Calling waitReady()");
      index.waitReady(Deadline.in(TIMEOUT_SHOULDNT)); // Arbitrary - might fail on slow or busy systems!
    } catch (TimeoutException e) {
      fail(String.format("Unexpected TimeoutException thrown: %s", e));
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testIndexArtifact() throws Exception {
    // Assert attempting to index a null ArtifactData throws an IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null artifact data", () -> index.indexArtifact(null));

    // Assert attempting to index an ArtifactData with null ArtifactIdentifier throws an IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "ArtifactData has null identifier", () -> {
      index.indexArtifact(new ArtifactData(null, null, null, null, null, null));
    });

    // Assert against variant scenario
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (!spec.isDeleted()) {
        assertTrue(index.artifactExists(spec.getArtifactId()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));
      } else {
        assertFalse(index.artifactExists(spec.getArtifactId()));
        assertNull(index.getArtifact(spec.getArtifactId()));
      }
    }

    // Create an artifact spec
    ArtifactSpec spec1 = ArtifactSpec.forCollAuUrl("c", "a", "u1").thenCommit();
    spec1.setArtifactId(UUID.randomUUID().toString());
    spec1.generateContent();
    spec1.setStorageUrl(URI.create("file:///tmp/a"));

    // Index the artifact and assert the Artifact the operation returns matches the spec
    Artifact indexed1 = index.indexArtifact(spec1.getArtifactData());
    spec1.assertArtifactCommon(indexed1);

    // Assert the artifact exists in the index and it matches the spec
    assertTrue(index.artifactExists(spec1.getArtifactId()));
    spec1.assertArtifactCommon(index.getArtifact(spec1.getArtifactId()));
  }

  /**
   * Asserts that an artifact's repository state is recorded accurately in the index.
   *
   * TODO: Move this to {@link ArtifactSpec#assertArtifact(LockssRepository, Artifact)}?
   *
   * @throws Exception
   */
  @Test
  public void testIndexArtfact_artifactRepoState() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactId(UUID.randomUUID().toString())
        .setCollection("collection")
        .setAuid("auid")
        .setUrl("url")
        .setVersion(1)
        .setCollectionDate(Instant.now().toEpochMilli())
        .setStorageUrl(new URI("storageUrl"))
        .setContentLength(1232L)
        .setCommitted(true)
        .setDeleted(false);

    spec.generateContent();

    ArtifactRepositoryState state =
        new ArtifactRepositoryState(spec.getArtifactIdentifier(), spec.isCommitted(), spec.isDeleted());

    ArtifactData ad = spec.getArtifactData();
    ad.setArtifactRepositoryState(state);

    Artifact indexed = index.indexArtifact(ad);

    assertEquals(spec.isCommitted(), indexed.isCommitted());
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifact_artifactId() throws Exception {
    // Assert retrieving an artifact with a null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact ID", () -> index.getArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID", () -> index.getArtifact((UUID) null));

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isToDelete()) {
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactId()));
        assertNull(index.getArtifact(spec.getArtifactId()));
      } else {
        assertFalse(spec.isDeleted());
        assertTrue(index.artifactExists(spec.getArtifactId()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));
      }
    }

    // Assert retrieving a non-existent artifact ID returns null (String and UUID variants)
    assertNull(index.getArtifact("unknown"));
    assertNull(index.getArtifact(UUID.randomUUID()));

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactId()));
    assertNull(index.getArtifact(spec.getArtifactId()));
    assertNull(index.getArtifact(UUID.fromString(spec.getArtifactId())));

    // Index the artifact
    Artifact indexed = index.indexArtifact(spec.getArtifactData());

    // Sanity check
    spec.assertArtifactCommon(indexed);

    // Retrieve artifact from index and assert it matches the spec
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));
    spec.assertArtifactCommon(index.getArtifact(UUID.fromString(spec.getArtifactId())));
  }

  @Test
  public void testGetArtifact_latestCommitted() throws Exception {
    String COLLECTION = "d";
    String AU_A = "a";
    String AU_B = "b";
    String URL_U = "u";
    String URL_V = "v";

    // Create ArtifactSpecs with multiple versions; keep track of highest version in variant state
    List<ArtifactSpec> specs = new ArrayList<>();
    specs.add(createArtifactSpec(COLLECTION, AU_A, URL_U, 1, false));
    specs.add(createArtifactSpec(COLLECTION, AU_A, URL_U, 2, false));

    specs.add(createArtifactSpec(COLLECTION, AU_B, URL_U, 1, true));
    specs.add(createArtifactSpec(COLLECTION, AU_B, URL_U, 2, false));

    specs.add(createArtifactSpec(COLLECTION, AU_B, URL_V, 1, true));
    specs.add(createArtifactSpec(COLLECTION, AU_B, URL_V, 2, false));
    specs.add(createArtifactSpec(COLLECTION, AU_B, URL_V, 3, true));

    // Populate index and update variant state
    populateIndex(index, specs);

    // Used as handle to getArtifact(...) result
    ArtifactSpec spec;

    // A_U

    assertNull(variantState.getLatestArtifactSpec(COLLECTION, AU_A, URL_U, false));
    assertNull(index.getArtifact(COLLECTION, AU_A, URL_U, false));
//    assertLatestArtifactVersion(COLLECTION, AU_A, URL_U, false, -1);
    assertLatestArtifactVersion(COLLECTION, AU_A, URL_U, true, 2);

    // Commit a_u_1
    spec = specs.get(0);
    index.commitArtifact(spec.getArtifactId());
    variantState.commit(spec.getArtifactId());

    assertLatestArtifactVersion(COLLECTION, AU_A, URL_U, false, 1);
    assertLatestArtifactVersion(COLLECTION, AU_A, URL_U, true, 2);

    //// B_U

    assertLatestArtifactVersion(COLLECTION, AU_B, URL_U, false, 1);
    assertLatestArtifactVersion(COLLECTION, AU_B, URL_U, true, 2);

    // Commit b_u_2
    spec = specs.get(3);
    index.commitArtifact(spec.getArtifactId());
    variantState.commit(spec.getArtifactId());

    assertLatestArtifactVersion(COLLECTION, AU_B, URL_U, false, 2);
    assertLatestArtifactVersion(COLLECTION, AU_B, URL_U, true, 2);

    //// B_V

    assertLatestArtifactVersion(COLLECTION, AU_B, URL_V, false, 3);
    assertLatestArtifactVersion(COLLECTION, AU_B, URL_V, true, 3);

    // Commit b_v_2
    spec = specs.get(5);
    index.commitArtifact(spec.getArtifactId());
    variantState.commit(spec.getArtifactId());

    assertLatestArtifactVersion(COLLECTION, AU_B, URL_V, false, 3);
    assertLatestArtifactVersion(COLLECTION, AU_B, URL_V, true, 3);
  }

  public void assertLatestArtifactVersion(String collection, String auid, String uri, boolean includeUncommitted, int expectedVersion) throws Exception {
    // Get latest version Artifact from variant state and index
    Artifact expected = variantState.getLatestArtifactSpec(collection, auid, uri, includeUncommitted).getArtifact();
    Artifact actual = index.getArtifact(collection, auid, uri, includeUncommitted);

    // Assert latest Artifact from variant and index match
    assertEquals(expected, actual);

    // Sanity check
    assertEquals(expectedVersion, (int) expected.getVersion());
    assertEquals(expectedVersion, (int) actual.getVersion());
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifacts() throws Exception {
    List<Artifact> expected;

    // Assert variant state
    for (String collection : variantState.allCollections()) {
      for (String auid : variantState.allAuids(collection)) {
        // With includeUncommitted set to false
        expected = variantState.getArtifactsFrom(variantState.getLatestArtifactSpecs(collection, auid, false));
        assertIterableEquals(expected, index.getArtifacts(collection, auid, false));

        // With includeUncommitted set to true
        expected = variantState.getArtifactsFrom(variantState.getLatestArtifactSpecs(collection, auid, true));
        assertIterableEquals(expected, index.getArtifacts(collection, auid, true));
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersions_forUrl() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 1, false));
    specs.add(createArtifactSpec("d", "a", "u", 2, false));
    specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    specs.add(createArtifactSpec("d", "a", "u2", 1, true));
    specs.add(createArtifactSpec("d", "a", "v", 1, false));
    specs.add(createArtifactSpec("d", "a", "v", 2, true));

    populateIndex(index, specs);

    assertEmpty(index.getArtifactsAllVersions("d", "a", "u"));

    assertIterableEquals(artList(specs, 5), index.getArtifactsAllVersions("d", "a", "v"));

    index.commitArtifact(specs.get(4).getArtifactId());
    variantState.commit(specs.get(4).getArtifactId());

    log.debug("result = {}", ListUtil.fromIterable(index.getArtifactsAllVersions("d", "a", "v")));
    assertIterableEquals(artList(specs, 5, 4), index.getArtifactsAllVersions("d", "a", "v"));
  }

  @Test
  public void testGetArtifactsAllVersionsAllAus() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 2, true));
    specs.add(createArtifactSpec("d", "a", "u", 1, true));
    specs.add(createArtifactSpec("d", "a", "v", 1, true));
    specs.add(createArtifactSpec("d", "b", "u", 1, false));
    specs.add(createArtifactSpec("d", "b", "v", 1, true));
    specs.add(createArtifactSpec("d", "a", "u", 3, false));
    specs.add(createArtifactSpec("d", "b", "u", 2, false));

    populateIndex(index, specs);

    //// Assert unknown or null collections and URLs result in an empty set
    assertEmpty(index.getArtifactsWithUrlFromAllAus(null, null));
    assertEmpty(index.getArtifactsWithUrlFromAllAus("d", null));
    assertEmpty(index.getArtifactsWithUrlFromAllAus(null, "u"));

    //// Demonstrate committing an artifact affects the result
    assertIterableEquals(
        artList(specs, 0, 1),
        index.getArtifactsWithUrlFromAllAus("d", "u")
    );

    // Commit all uncommitted artifacts
    for (ArtifactSpec spec : specs) {
      if (!spec.isCommitted()) {
        index.commitArtifact(spec.getArtifactId());
        variantState.commit(spec.getArtifactId());
      }
    }

    // Verify proper order
    assertIterableEquals(
        artList(specs, 5, 0, 1, 6, 3),
        index.getArtifactsWithUrlFromAllAus("d", "u")
    );

    //// Demonstrate deleting an artifact affects the result
    assertIterableEquals(
        artList(specs, 2, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v")
    );

    index.deleteArtifact(specs.get(2).getArtifactId());

    assertIterableEquals(
        artList(specs, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v")
    );

    index.deleteArtifact(specs.get(4).getArtifactId());

    assertEmpty(index.getArtifactsWithUrlFromAllAus("d", "v"));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsWithPrefix() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 1, true));
    specs.add(createArtifactSpec("d", "a", "u", 2, false));
    specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    specs.add(createArtifactSpec("d", "a", "u2", 1, true));
    specs.add(createArtifactSpec("d", "a", "v", 1, false));
    specs.add(createArtifactSpec("d", "a", "v", 2, true));

    populateIndex(index, specs);

    // Assert an unknown
    assertEmpty(index.getArtifactsWithPrefix("c", "a", "w"));

    assertEquals(4, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "")));
    assertEquals(3, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u")));
    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "v")));

    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u1")));
    assertEquals(0, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "ux")));

    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u2")));
    assertEquals(0, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u2x")));

    ArtifactSpec spec = specs.get(1);
    spec.setCommitted(true);
    index.commitArtifact(spec.getArtifactId());
    variantState.commit(spec.getArtifactId());

    assertEquals(3, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u")));

  }

  @Test
  public void testGetArtifactsWithPrefixAllVersionsAllAus() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 2, true));
    specs.add(createArtifactSpec("d", "a", "u", 1, true));
    specs.add(createArtifactSpec("d", "a", "u1", 3, true));
    specs.add(createArtifactSpec("d", "a", "u1", 2, true));
    specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    specs.add(createArtifactSpec("d", "a", "u2", 2, true));
    specs.add(createArtifactSpec("d", "a", "u2", 1, false));
    specs.add(createArtifactSpec("d", "a", "v", 1, true));
    specs.add(createArtifactSpec("d", "b", "u", 1, false));
    specs.add(createArtifactSpec("d", "b", "v", 1, true));
    specs.add(createArtifactSpec("d", "a", "u", 3, false));
    specs.add(createArtifactSpec("d", "b", "u", 2, false));

    populateIndex(index, specs);

    //// Assert unknown or null collections and URLs result in an empty set
    assertEmpty(index.getArtifactsWithUrlPrefixFromAllAus(null, null));
    assertEmpty(index.getArtifactsWithUrlPrefixFromAllAus(null, "u"));

    // Assert a null prefix returns all the committed artifacts in the collection
    assertIterableEquals(
        specs.stream().filter(ArtifactSpec::isCommitted).map(ArtifactSpec::getArtifact).collect(Collectors.toList()),
        index.getArtifactsWithUrlPrefixFromAllAus("d", null)
    );

    assertGetArtifactsWithPrefixAllVersionsAllAus(specs, "d", "u");
    assertGetArtifactsWithPrefixAllVersionsAllAus(specs, "d", "u1");

    //// Assert affect of committing an artifact on result
    assertIterableEquals(
        artList(specs, 5),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2")
    );

    // Commit all uncommitted artifacts
    for (ArtifactSpec spec : specs) {
      if (!spec.isCommitted()) {
        index.commitArtifact(spec.getArtifactId());
        variantState.commit(spec.getArtifactId());
      }
    }

    // Verify proper order
    assertIterableEquals(
        artList(specs, 10, 0, 1, 11, 8, 2, 3, 4, 5, 6),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u")
    );

    assertIterableEquals(
        artList(specs, 5, 6),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2"));

    //// Assert affect of deleting an artifact on result
    assertIterableEquals(
        artList(specs, 2, 3, 4),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1")
    );

    index.deleteArtifact(specs.get(3).getArtifactId());
    variantState.delFromAll(specs.get(3));

    assertIterableEquals(
        artList(specs, 2, 4),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1")
    );
  }

  private void assertGetArtifactsWithPrefixAllVersionsAllAus(List<ArtifactSpec> specs, String collection, String prefix) throws IOException {
    assertIterableEquals(
        specs.stream()
            .filter(spec -> !spec.isDeleted())
            .filter(ArtifactSpec::isCommitted)
            .filter(spec -> spec.getCollection().equals(collection))
            .filter(spec -> spec.getUrl().startsWith(prefix))
            .map(ArtifactSpec::getArtifact)
            .collect(Collectors.toList()),
        index.getArtifactsWithUrlPrefixFromAllAus(collection, prefix)
    );
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactVersion() throws Exception {
    // Assert unknown artifact version keys causes getArtifactVersion(...) to return null
    assertNull(index.getArtifactVersion(null, null, null, 0));
    assertNull(index.getArtifactVersion("d", "a", "v", 1));

    Integer[] versions = {1, 2, 3, 41};
    Boolean[] commits = {true, true, false, true};

    // Populate the index with fixed multiple versions of the same artifact stem
    for (Pair<Integer, Boolean> pair : zip(versions, commits)) {
      indexArtifactSpec(createArtifactSpec("d", "a", "v", pair.getKey(), pair.getValue()));
    }

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      Artifact expected = spec.getArtifact();
      Artifact actual = index.getArtifactVersion(spec.getCollection(), spec.getAuid(), spec.getUrl(), spec.getVersion());

      if (spec.isCommitted()) {
        assertNotNull(actual);
        spec.assertArtifactCommon(actual);
        assertEquals(expected, actual);
      } else {
        assertNull(actual);
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testUpdateStorageUrl() throws Exception {
    // Attempt to update the storage URL of a null artifact ID
    assertThrowsMatch(IllegalArgumentException.class, "Invalid artifact ID", () -> index.updateStorageUrl(null, "xxx"));

    // Attempt to update the storage URL of an unknown artifact ID
    assertNull(index.updateStorageUrl("xyzzy", "xxx"));

    // Assert we're able to update the storage URLs of all existing artifacts
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      // Ensure the current storage URL matches the artifact specification in the variant state
      Artifact indexed1 = index.getArtifact(spec.getArtifactId());
      assertEquals(spec.getArtifact().getStorageUrl(), indexed1.getStorageUrl());

      // Update the storage URL
      String nsURL = UUID.randomUUID().toString();
      Artifact updated = index.updateStorageUrl(spec.getArtifactId(), nsURL);
      spec.setStorageUrl(URI.create(nsURL));

      // Assert the artifact returned by updateStorageUrl() reflects the new storage URL
      assertEquals(spec.getArtifact(), updated);
      assertEquals(spec.getArtifact().getStorageUrl(), updated.getStorageUrl());

      // Fetch the artifact from index and assert it matches
      Artifact indexed2 = index.getArtifact(spec.getArtifactId());
      assertEquals(spec.getArtifact(), indexed2);
      assertEquals(spec.getArtifact().getStorageUrl(), indexed2.getStorageUrl());
    }

    // Assert updating the storage URL of a deleted artifact returns null (unknown artifact)
    ArtifactSpec spec = variantState.anyDeletedSpec();
    if (spec != null) {
      assertNull(index.updateStorageUrl(spec.getArtifactId(), "xxx"));
    }
  }

  private static final int MAXIMUM_URL_LENGTH = 32766;

  @Test
  public void testLongUrls() throws Exception {
    // Build successively longer URLs (not really necessary but good for debugging)
    List<String> urls = IntStream.range(1, 14)
        .mapToObj(i -> RandomStringUtils.randomAlphanumeric((int) Math.pow(2, i)))
        .collect(Collectors.toList());

    // Add URL with maximum length
    urls.add(RandomStringUtils.randomAlphanumeric(MAXIMUM_URL_LENGTH));

    // Create and index artifact specs for each URL
    for (String url : urls) {
      ArtifactSpec spec = createArtifactSpec("c", "a", url, 1, false);
      indexArtifactSpec(spec);
    }

    // Ensure the URL of artifacts in the index matches the URL in their specification
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      Artifact indexed = index.getArtifact(spec.getArtifactId());
      assertEquals(spec.getUrl(), indexed.getUri());
    }

    /*
    try {
      // Create a URL one byte longer than the maximum length
      String url = RandomStringUtils.randomAscii(MAXIMUM_URL_LENGTH + 1);

      // Attempt to index the artifact - should throw
      ArtifactSpec spec = createArtifactSpec("c", "a", url, 1, false);
      indexArtifactSpec(spec);

      // Fail if we reach here
      fail("Expected createArtifactSpec(...) to throw an Exception");
    } catch (SolrException e) {
      // OK
    }
    */
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testAuSize() throws Exception {
    // Check AU size of non-existent AUs
    assertEquals(0L, index.auSize(null, null).longValue());
    assertEquals(0L, index.auSize("collection", null).longValue());
    assertEquals(0L, index.auSize(null, "auid").longValue());
    assertEquals(0L, index.auSize("collection", "auid").longValue());

    // Assert variant state
    for (String collection : variantState.allCollections()) {
      for (String auid : variantState.allAuids(collection)) {
        log.debug("index.auSize() = {}", index.auSize(collection, auid));
        log.debug("variantState.auSize() = {}", variantState.auSize(collection, auid));
        assertEquals((long) variantState.auSize(collection, auid), (long) index.auSize(collection, auid));
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testCommitArtifact() throws Exception {
    // Assert committing an artifact with a null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact ID", () -> index.commitArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID", () -> index.commitArtifact((UUID) null));

    // Assert committing an artifact of unknown artifact ID returns null
    assertNull(index.commitArtifact("unknown"));
    assertNull(index.commitArtifact(UUID.randomUUID()));

    // Assert against variant scenario
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {

      // Assert index
      if (!spec.isToDelete()) {
        // Sanity check
        assertFalse(spec.isDeleted());

        // Assert artifact exists in the index and that it matches the spec
        assertTrue(index.artifactExists(spec.getArtifactId()));
        Artifact artifact = index.getArtifact(spec.getArtifactId());
        spec.assertArtifactCommon(artifact);

        // Assert committed state of artifact against spec
        if (spec.isToCommit()) {
          assertTrue(spec.isCommitted());
          assertTrue(artifact.getCommitted());
        } else {
          assertFalse(spec.isCommitted());
          assertFalse(artifact.getCommitted());
        }
      } else {
        // Assert deleted artifact states
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactId()));
        assertNull(index.getArtifact(spec.getArtifactId()));
      }

      // Commit (again)
      Artifact indexed = index.commitArtifact(spec.getArtifactId());
      spec.setCommitted(true);

      if (!spec.isDeleted()) {
        // Assert artifact is committed
        assertNotNull(indexed);
        spec.assertArtifactCommon(indexed);
        assertTrue(indexed.getCommitted());
      } else {
        // Assert commit operation on a non-existent, deleted artifact returns null
        assertNull(indexed);
        assertFalse(index.artifactExists(spec.getArtifactId()));
        assertNull(index.getArtifact(spec.getArtifactId()));
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testDeleteArtifact() throws Exception {
    // Assert deleting an artifact with null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty identifier", () -> index.deleteArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID", () -> index.deleteArtifact((UUID) null));

    // Assert removing a non-existent artifact ID returns false (String and UUID)
    assertFalse(index.deleteArtifact("unknown"));
    assertFalse(index.deleteArtifact(UUID.randomUUID()));

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      assertEquals(spec.isDeleted(), !index.artifactExists(spec.getArtifactId()));
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactId()));
    assertNull(index.getArtifact(spec.getArtifactId()));
    assertFalse(index.deleteArtifact(spec.getArtifactId()));

    // Index the artifact data from spec and assert the return matches spec
    Artifact indexed = index.indexArtifact(spec.getArtifactData());
    spec.assertArtifactCommon(indexed);

    // Assert conditions expected if the artifact is in the index
    assertTrue(index.artifactExists(spec.getArtifactId()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));

    // Delete from index and assert operation was successful (returns true)
    assertTrue(index.deleteArtifact(spec.getArtifactId()));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactId()));
    assertNull(index.getArtifact(spec.getArtifactId()));
    assertFalse(index.deleteArtifact(spec.getArtifactId()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testArtifactExists() throws Exception {
    // Attempt calling artifactExists() with null artifact ID; should throw IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact ID", () -> index.artifactExists(null));

    // Assert artifactExists() returns false for an artifact with an unknown artifact ID
    assertFalse(index.artifactExists("unknown"));

    // Assert result of artifactExists() depending on variant state and spec
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isToDelete()) {
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactId()));
        assertNull(index.getArtifact(spec.getArtifactId()));
      } else {
        assertFalse(spec.isDeleted());
        assertTrue(index.artifactExists(spec.getArtifactId()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));
      }
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert that an artifact by the spec's artifact ID does not exist yet
    assertFalse(index.artifactExists(spec.getArtifactId()));

    // Add the artifact to the index
    index.indexArtifact(spec.getArtifactData());

    // Assert that an artifact by the spec's artifact ID now exists
    assertTrue(index.artifactExists(spec.getArtifactId()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactId()));

    // Remove the artifact from the index
    index.deleteArtifact(spec.getArtifactId());

    // Assert that an artifact by the spec's artifact ID no longer exists
    assertFalse(index.artifactExists(spec.getArtifactId()));
    assertNull(index.getArtifact(spec.getArtifactId()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetCollectionIds() throws Exception {
    // Assert that the collection IDs from the index matches the variant scenario state
    assertIterableEquals(variantState.activeCollections(), index.getCollectionIds());

    // Create an artifact spec
    ArtifactSpec spec1 = ArtifactSpec.forCollAuUrl("xyzzy", "foo", "bar");
    spec1.setArtifactId(UUID.randomUUID().toString());
    spec1.generateContent();
    spec1.setStorageUrl(URI.create("file:///tmp/a"));

    // Sanity check
    assertFalse(spec1.isToCommit());
    assertFalse(spec1.isCommitted());

    // Index artifact from artifact spec
    Artifact indexed = index.indexArtifact(spec1.getArtifactData());

    // Sanity check
    assertFalse(indexed.getCommitted());
    Artifact retrieved = index.getArtifact(spec1.getArtifactId());
    assertFalse(retrieved.getCommitted());

    // Assert the uncommitted artifact's collection ID is included
    assertTrue(IterableUtils.contains(index.getCollectionIds(), spec1.getCollection()));

    // Commit artifact
    index.commitArtifact(spec1.getArtifactId());

    // Assert the committed artifact's collection ID is now included
    assertTrue(IterableUtils.contains(index.getCollectionIds(), spec1.getCollection()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetAuIds() throws Exception {
    // Assert calling getAuIds() with a null or unknown collection ID returns an empty set
    assertTrue(IterableUtils.isEmpty(index.getAuIds(null)));
    assertTrue(IterableUtils.isEmpty(index.getAuIds("unknown")));

    // Assert variant scenario
    for (String collectionId : variantState.activeCommittedCollections()) {
      assertIterableEquals(variantState.activeAuids(collectionId), index.getAuIds(collectionId));
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl("xyzzy", "foo", "bar");
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Index the artifact
    Artifact indexed = index.indexArtifact(spec.getArtifactData());
    spec.assertArtifactCommon(indexed);

    // Assert that set of AUIDs for the collection is not empty until the artifact is committed
    assertFalse(IterableUtils.isEmpty(index.getAuIds(spec.getCollection())));

    // Commit the artifact
    Artifact committed = index.commitArtifact(spec.getArtifactId());

    // Sanity check
    assertTrue(committed.getCommitted());
    spec.setCommitted(true);
    spec.assertArtifactCommon(committed);

    // Assert that set of AUIDs for the collection is non-empty
    assertFalse(IterableUtils.isEmpty(index.getAuIds(spec.getCollection())));
    assertIterableEquals(Collections.singleton(spec.getAuid()), index.getAuIds(spec.getCollection()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersions_inAU() throws Exception {
    String COLLECTION = "d";
    String AU_A = "a";
    String AU_B = "b";

    // Assert null and unknown collection/AUIDs return an empty result (i.e., no matches)
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, null, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, null, false)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(COLLECTION, null, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, AU_A, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, AU_B, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(COLLECTION, AU_A, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(COLLECTION, AU_B, true)));

    List<ArtifactSpec> specs = new ArrayList<>();
    ArtifactSpec spec_a_1 = createArtifactSpec(COLLECTION, AU_A, "u", 1, false);
    ArtifactSpec spec_b_1 = createArtifactSpec(COLLECTION, AU_B, "u", 1, false);
    ArtifactSpec spec_b_2 = createArtifactSpec(COLLECTION, AU_B, "v", 1, true);

    specs.add(spec_a_1);
    specs.add(spec_b_1);
    specs.add(spec_b_2);

    // Index the artifact from specs
    populateIndex(index, specs);

    // Used as handle to results from getArtifactsAllVersions(...)
    List<Artifact> result;

    // Used as handle to single Artifact result
    Artifact artifact;

    //// FIRST AU

    // Assert empty result if includeUncommitted is set to false
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(COLLECTION, AU_A, false)));

    // Assert we get back one artifact if we set includeUncommitted to true
    result = IterableUtils.toList(index.getArtifactsAllVersions(COLLECTION, AU_A, true));
    assertEquals(1, result.size());
    spec_a_1.assertArtifactCommon(result.get(0));

    // Commit artifact
    index.commitArtifact(spec_a_1.getArtifactId());
    spec_a_1.setCommitted(true);

    // Sanity check
    artifact = index.getArtifact(spec_a_1.getArtifactId());
    assertTrue(artifact.getCommitted());

    // Assert we now get back one artifact even if includeUncommitted is false
    result = IterableUtils.toList(index.getArtifactsAllVersions(COLLECTION, AU_A, false));
    assertEquals(1, result.size());
    spec_a_1.assertArtifactCommon(result.get(0));

    //// SECOND AU

    // Assert we get back the committed artifact with includeUncommitted set to false
    result = IterableUtils.toList(index.getArtifactsAllVersions(COLLECTION, AU_B, false));
    assertEquals(1, result.size());
    spec_b_2.assertArtifactCommon(result.get(0));

    // Assert we get back both artifacts with includeUncommitted set to true
    result = IterableUtils.toList(index.getArtifactsAllVersions(COLLECTION, AU_B, true));
    assertEquals(2, result.size());
    assertIterableEquals(ListUtil.list(spec_b_1.getArtifact(), spec_b_2.getArtifact()), result);

    // Commit artifact
    index.commitArtifact(spec_b_1.getArtifactId());
    spec_b_1.setCommitted(true);

    // Sanity check
    artifact = index.getArtifact(spec_b_1.getArtifactId());
    assertTrue(artifact.getCommitted());

    // Assert we now get back both artifact with includeUncommitted set to false
    result = IterableUtils.toList(index.getArtifactsAllVersions(COLLECTION, AU_B, false));
    assertEquals(2, result.size());
    assertIterableEquals(ListUtil.list(spec_b_1.getArtifact(), spec_b_2.getArtifact()), result);

    // Assert variant state
    for (String collection : variantState.allCollections()) {
      for (String auid : variantState.allAuids(collection)) {
        result = IterableUtils.toList(index.getArtifactsAllVersions(collection, auid, false));
        assertIterableEquals(variantState.getArtifactsFrom(variantState.getArtifactsAllVersions(collection, auid, false)), result);

        result = IterableUtils.toList(index.getArtifactsAllVersions(collection, auid, true));
        assertIterableEquals(variantState.getArtifactsFrom(variantState.getArtifactsAllVersions(collection, auid, true)), result);
      }
    }

    // Get any uncommitted ArtifactSpec from variant state (or null if one doesn't exist / isn't available)
    ArtifactSpec spec = variantState.anyUncommittedSpec();

    if (spec != null) {
      // Sanity check
      assertFalse(spec.isDeleted());
      assertFalse(spec.isCommitted());

      // Assert the uncommitted artifact *is not* included in the results for its AU with includeUncommitted set to false
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getCollection(), spec.getAuid(), false));
      assertFalse(result.contains(spec.getArtifact()));

      // Assert the uncommitted artifact *is* included in the results for its AU with includeUncommitted set to true
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getCollection(), spec.getAuid(), true));
      assertTrue(result.contains(spec.getArtifact()));

      // Commit the artifact
      index.commitArtifact(spec.getArtifactId());
      spec.setCommitted(true);

      // Assert the uncommitted artifact *is now* included in the results for its AU with includeUncommitted set to false
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getCollection(), spec.getAuid(), false));
      assertTrue(result.contains(spec.getArtifact()));
    }
  }

  // *******************************************************************************************************************
  // * STATIC UTILITY METHODS
  // *******************************************************************************************************************

  public static <A, B> List<Pair<A, B>> zip(A[] a, B[] b) {
    return zip(ListUtil.list(a), ListUtil.list(b));
  }

  public static <A, B> List<Pair<A, B>> zip(List<A> a, List<B> b) {
    return IntStream.range(0, Math.min(a.size(), b.size()))
        .mapToObj(i -> Pair.of(a.get(i), b.get(i)))
        .collect(Collectors.toList());
  }

  private static ArtifactSpec createArtifactSpec(String collection, String auid, String uri, long version, boolean commit) {
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(collection, auid, uri);

    spec.setArtifactId(UUID.randomUUID().toString());
    spec.setVersion((int) version);

    if (commit) {
      spec.thenCommit();
    }

    return spec;
  }

  /** Return list of Artifacts correcponding to the selected elements of
   * the spec list */
  private static List<Artifact> artList(List<ArtifactSpec> specs,
                                        int... indices) {
      List<Artifact> res = new ArrayList<>();
      for (int ix : indices) {
        res.add(specs.get(ix).getArtifact());
      }
      return res;
    }

}
