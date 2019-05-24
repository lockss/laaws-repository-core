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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.ListUtil;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.test.VariantTest;

import java.io.IOException;
import java.util.*;

public abstract class AbstractArtifactIndexTest<AI extends ArtifactIndex> extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  protected UUID uuid;
  protected ArtifactIdentifier aid1;
  protected ArtifactIdentifier aid2;
  protected ArtifactIdentifier aid3;
  protected RepositoryArtifactMetadata md1;
  protected RepositoryArtifactMetadata md2;
  protected RepositoryArtifactMetadata md3;
  protected ArtifactData artifact1;
  protected ArtifactData artifact2;
  protected ArtifactData artifact3;

  protected AI index;

  // *******************************************************************************************************************
  // * ABSTRACT METHODS
  // *******************************************************************************************************************

  protected abstract AI makeArtifactIndex() throws IOException;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @BeforeEach
  public void setupCommon() throws Exception {
    uuid = UUID.randomUUID();

    aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", 1);
    aid2 = new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", 2);
    aid3 = new ArtifactIdentifier("id3", "coll1", "auid1", "uri3", 2);

    md1 = new RepositoryArtifactMetadata(aid1, false, false);
    md2 = new RepositoryArtifactMetadata(aid2, true, false);
    md3 = new RepositoryArtifactMetadata(aid2, true, false);

    artifact1 = new ArtifactData(aid1, null, null, null, "surl1", md1);
    artifact2 = new ArtifactData(aid2, null, null, null, "surl2", md2);
    artifact3 = new ArtifactData(aid3, null, null, null, "surl3", md3);

    index = makeArtifactIndex();

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

  protected void indexArtifactSpec(AI index, ArtifactSpec spec) throws IOException {
    // Generate content if needed (Artifact objects must represent)
    if (!spec.hasContent()) {
      spec.generateContent();
      spec.setStorageUrl(spec.getArtifactId());
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

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testIndexArtifact() throws Exception {
    String expectedMessage = "Null artifact";

    // Assert attempting to index a null ArtifactData throws an IllegalArgumentException
    try {
      index.indexArtifact(null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    expectedMessage = "ArtifactData has null identifier";

    // Assert attempting to index an ArtifactData with null ArtifactIdentifier throws an IllegalArgumentException
    try {
      index.indexArtifact(new ArtifactData(null, null, null, null, null, null));
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

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
    spec1.setStorageUrl("file:///tmp/a");

    // Index the artifact and assert the Artifact the operation returns matches the spec
    Artifact indexed1 = index.indexArtifact(spec1.getArtifactData());
    spec1.assertArtifactCommon(indexed1);

    // Assert the artifact exists in the index and it matches the spec
    assertTrue(index.artifactExists(spec1.getArtifactId()));
    spec1.assertArtifactCommon(index.getArtifact(spec1.getArtifactId()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifact_artifactId() throws Exception {
    String expectedMessage = null;

    // Assert retrieving an artifact with a null artifact ID (String) throws IllegalArgumentException
    try {
      expectedMessage = "Null or empty identifier";
      index.getArtifact((String) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

    // Assert retrieving an artifact with a null artifact ID (UUID) throws IllegalArgumentException
    try {
      expectedMessage = "Null UUID";
      index.getArtifact((UUID) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

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
    spec.setStorageUrl("file:///tmp/a");

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

//  @VariantTest
//  @EnumSource(TestIndexScenarios.class)
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

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifacts() throws Exception {

    for (String collection : variantState.allCollections()) {
      for (String auid : variantState.allAuids(collection)) {

        List<Artifact> expected = variantState.getArtifactsFrom(variantState.getLatestArtifactSpecs(collection, auid, false));
        assertIterableEquals(expected, index.getArtifacts(collection, auid, false));

        // WIP

      }
    }

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersions_withUrl() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersionsAllAus() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsWithPrefix() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsWithPrefixAllVersionsAllAus() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactVersion() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testInitIndex() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testShutdownIndex() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testUpdateStorageUrl() throws Exception {

  }

  @Disabled
  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testWaitReady() throws Exception {

  }

  @Disabled
  @Test
  public void testAuSize(String collection, String auid) {
    // TODO
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testCommitArtifact() throws Exception {
    String expectedMessage = "Null or empty identifier";

    // Assert committing an artifact with a null artifact ID (String) throws IllegalArgumentException
    try {
      index.commitArtifact((String) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

    // Assert committing an artifact with a null artifact ID (UUID) throws IllegalArgumentException
    try {
      expectedMessage = "Null UUID";
      index.commitArtifact((UUID) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

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
    String expectedMessage = "Null or empty identifier";

    // Assert deleting an artifact with null artifact ID (String) throws IllegalArgumentException
    try {
      index.deleteArtifact((String) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

    // Assert deleting an artifact with null artifact ID (UUID) throws IllegalArgumentException
    try {
      expectedMessage = "Null UUID";
      index.deleteArtifact((UUID) null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

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
    spec.setStorageUrl("file:///tmp/a");

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
    String expectedMessage = "Null or empty identifier";

    // Attempt calling artifactExists() with null artifact ID; should throw IllegalArgumentException
    try {
      assertNotNull(index);
      index.artifactExists(null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage + ")");
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMessage, e.getMessage());
    }

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
    spec.setStorageUrl("file:///tmp/a");

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
    spec1.setStorageUrl("file:///tmp/a");

    // Sanity check
    assertFalse(spec1.isToCommit());
    assertFalse(spec1.isCommitted());

    // Index artifact from artifact spec
    Artifact indexed = index.indexArtifact(spec1.getArtifactData());

    // Sanity check
    assertFalse(indexed.getCommitted());
    Artifact retrieved = index.getArtifact(spec1.getArtifactId());
    assertFalse(retrieved.getCommitted());

    // Assert the uncommitted artifact's collection ID is not included
    assertFalse(IterableUtils.contains(index.getCollectionIds(), spec1.getCollection()));

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
    spec.setStorageUrl("file:///tmp/a");

    // Index the artifact
    Artifact indexed = index.indexArtifact(spec.getArtifactData());
    spec.assertArtifactCommon(indexed);

    // Assert that set of AUIDs for the collection is empty until the artifact is committed
    assertTrue(IterableUtils.isEmpty(index.getAuIds(spec.getCollection())));

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

  private ArtifactSpec createArtifactSpec(String collection, String auid, String uri, long version, boolean commit) {
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(collection, auid, uri);

    spec.setArtifactId(UUID.randomUUID().toString());
    spec.setVersion((int) version);

    if (commit) {
      spec.thenCommit();
    }

    return spec;
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

  // TODO: Remove? Is this still needed?
  @Disabled // TODO: WIP
  @Test
  public void testGetArtifactsWithPrefixAllVersions() throws Exception {
    // Empty index
    assertFalse(index.getArtifactsWithPrefixAllVersions(null, null, null).iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", null, null).iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", "auid1", null).iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1").iterator().hasNext());

    // Index artifact1
    index.indexArtifact(artifact1);

    // Before committing artifac/t1
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1").iterator().hasNext());

    // Commit artifact1
    index.commitArtifact("id1");

    // After committing artifact1

    // Prefix "uri" yields "uri1"
    Iterator<Artifact> iter1 = index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri").iterator();
    assertTrue(iter1.hasNext());
    Artifact art1 = iter1.next();
    assertEquals("id1", art1.getId());
    assertEquals("coll1", art1.getCollection());
    assertEquals("auid1", art1.getAuid());
    assertEquals("uri1", art1.getUri());
    assertFalse(iter1.hasNext());

    // Prefix "uri1" yields "uri1"
    Iterator<Artifact> iter2 = index.getArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1").iterator();
    assertTrue(iter2.hasNext());
    Artifact art2 = iter2.next();
    assertEquals("id1", art2.getId());
    assertEquals("coll1", art2.getCollection());
    assertEquals("auid1", art2.getAuid());
    assertEquals("uri1", art2.getUri());
    assertFalse(iter2.hasNext());

    // Failed retrievals
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", null, null).iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", null).iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2").iterator().hasNext());

    // Index artifact2
    index.indexArtifact(artifact2);

    // Before committing artifact2
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri2").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2").iterator().hasNext());

    // Commit artifact2
    index.commitArtifact(uuid.toString());

    // After committing artifact2

    // Failed retrievals
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid1", "uri2").iterator().hasNext());
    assertFalse(index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").iterator().hasNext());

    // Prefix "uri" yields "uri2"
    Iterator<Artifact> iter3 = index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri").iterator();
    assertTrue(iter3.hasNext());
    Artifact art3 = iter3.next();
    assertEquals(uuid.toString(), art3.getId());
    assertEquals("coll2", art3.getCollection());
    assertEquals("auid2", art3.getAuid());
    assertEquals("uri2", art3.getUri());
    assertFalse(iter3.hasNext());

    // Prefix "uri2" yields "uri2"
    Iterator<Artifact> iter4 = index.getArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2").iterator();
    assertTrue(iter4.hasNext());
    Artifact art4 = iter4.next();
    assertEquals(uuid.toString(), art4.getId());
    assertEquals("coll2", art4.getCollection());
    assertEquals("auid2", art4.getAuid());
    assertEquals("uri2", art4.getUri());
    assertFalse(iter4.hasNext());
  }

  // TODO: Remove? It is unknown what function this originally tested
  @Disabled
  @Test
  public void testGetArtifactAllVersions() throws Exception {
    // Empty index
    assertFalse(index.getArtifactsAllVersions(null, null, null).iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll1", null, null).iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", null).iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", "uri1").iterator().hasNext());

    // Index artifact1
    index.indexArtifact(artifact1);

    // Before committing artfact1
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", "uri1").iterator().hasNext());

    // Commit artifact1
    index.commitArtifact("id1");

    // After committing artifact1
    assertFalse(index.getArtifactsAllVersions("coll1", "auid1", "uri").iterator().hasNext());

    Iterator<Artifact> iter1 = index.getArtifactsAllVersions("coll1", "auid1", "uri1").iterator();
    assertTrue(iter1.hasNext());
    Artifact art1 = iter1.next();
    assertEquals("id1", art1.getId());
    assertEquals("coll1", art1.getCollection());
    assertEquals("auid1", art1.getAuid());
    assertEquals("uri1", art1.getUri());
    assertFalse(iter1.hasNext());

    // Before indexing artifact2
    assertFalse(index.getArtifactsAllVersions("coll2", null, null).iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", null).iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri2").iterator().hasNext());

    // Index artifact2
    index.indexArtifact(artifact2);

    // Before committing artifact2
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri2").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri2").iterator().hasNext());

    // Commit artifact2
    index.commitArtifact(uuid.toString());

    // After committing artifact2
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri1").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid1", "uri2").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri").iterator().hasNext());
    assertFalse(index.getArtifactsAllVersions("coll2", "auid2", "uri1").iterator().hasNext());

    Iterator<Artifact> iter2 = index.getArtifactsAllVersions("coll2", "auid2", "uri2").iterator();
    assertTrue(iter2.hasNext());
    Artifact art2 = iter2.next();
    assertEquals(uuid.toString(), art2.getId());
    assertEquals("coll2", art2.getCollection());
    assertEquals("auid2", art2.getAuid());
    assertEquals("uri2", art2.getUri());
    assertFalse(iter2.hasNext());

    // Failed retrievals
    assertNull(index.getArtifactVersion("coll1", "auid1", "uri", 2));
    assertNull(index.getArtifactVersion("coll1", "auid1", "uri1", 2));
    assertNull(index.getArtifactVersion("coll1", "auid1", "uri", 1));

    // Successful retrieval
    Artifact art3 = index.getArtifactVersion("coll1", "auid1", "uri1", 1);
    assertEquals("id1", art3.getId());
    assertEquals("coll1", art3.getCollection());
    assertEquals("auid1", art3.getAuid());
    assertEquals("uri1", art3.getUri());
    assertEquals(1, (int)art3.getVersion());

    // Failed retrievals
    assertNull(index.getArtifactVersion("coll2", "auid2", "uri", 1));
    assertNull(index.getArtifactVersion("coll2", "auid2", "uri2", 1));
    assertNull(index.getArtifactVersion("coll2", "auid2", "uri", 2));

    // Successful retrieval
    Artifact art4 = index.getArtifactVersion("coll2", "auid2", "uri2", 2);
    assertNotNull(art4);
    assertEquals(uuid.toString(), art4.getId());
    assertEquals("coll2", art4.getCollection());
    assertEquals("auid2", art4.getAuid());
    assertEquals("uri2", art4.getUri());
    assertEquals(2, (int)art4.getVersion());
  }
}
