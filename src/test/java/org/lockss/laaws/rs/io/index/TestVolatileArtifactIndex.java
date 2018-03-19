/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.test.LockssTestCase4;

/**
 * Test class for {@code org.lockss.laaws.rs.io.index.VolatileArtifactIndex}
 */
public class TestVolatileArtifactIndex extends LockssTestCase4 {

  private ArtifactIdentifier aid1;
  private UUID uuid;
  private ArtifactIdentifier aid2;
  private RepositoryArtifactMetadata md1;
  private RepositoryArtifactMetadata md2;
  private ArtifactData artifact1;
  private ArtifactData artifact2;
  private VolatileArtifactIndex index;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", 1);
    uuid = UUID.randomUUID();
    aid2 = new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", 2);

    md1 = new RepositoryArtifactMetadata(aid1, false, false);
    md2 = new RepositoryArtifactMetadata(aid2, true, false);

    artifact1 = new ArtifactData(aid1, null, null, null, "surl1", md1);
    artifact2 = new ArtifactData(aid2, null, null, null, "surl2", md2);

    index = new VolatileArtifactIndex();
  }

  /*
   * Test methods.
   */
  @Test
  public void testIndexArtifact() {
    String expectedMessage = "Null artifact";

    try {
      index.indexArtifact(null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    expectedMessage = "ArtifactData has null identifier";

    try {
      index.indexArtifact(new ArtifactData(null, null, null, null, null, null));
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    Artifact aidata = index.indexArtifact(artifact1);

    assertEquals("id1", aidata.getId());
    assertEquals("coll1", aidata.getCollection());
    assertEquals("auid1", aidata.getAuid());
    assertEquals("uri1", aidata.getUri());
    assertEquals(1, (int)aidata.getVersion());
    assertEquals("surl1", aidata.getStorageUrl());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifact("id1"));

    aidata = index.indexArtifact(artifact2);

    assertEquals(uuid.toString(), aidata.getId());
    assertEquals("coll2", aidata.getCollection());
    assertEquals("auid2", aidata.getAuid());
    assertEquals("uri2", aidata.getUri());
    assertEquals(2, (int)aidata.getVersion());
    assertEquals("surl2", aidata.getStorageUrl());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifact(uuid.toString()));

    aidata = index.indexArtifact(artifact2);

    assertEquals(uuid.toString(), aidata.getId());
    assertEquals("coll2", aidata.getCollection());
    assertEquals("auid2", aidata.getAuid());
    assertEquals("uri2", aidata.getUri());
    assertEquals(2, (int)aidata.getVersion());
    assertEquals("surl2", aidata.getStorageUrl());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifact(uuid.toString()));
  }

  @Test
  public void testGetArtifactIndexData() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.getArtifact(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID nullIdUuid = null;
      expectedMessage = "Null UUID";
      index.getArtifact(nullIdUuid);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertNull(index.getArtifact("id1"));
    Artifact aidata1 = index.indexArtifact(artifact1);
    assertEquals(aidata1, index.getArtifact("id1"));

    assertNull(index.getArtifact(uuid));
    Artifact aidata2 = index.indexArtifact(artifact2);
    assertEquals(aidata2, index.getArtifact(uuid));

    aidata1 = index.indexArtifact(artifact1);
    assertEquals(aidata1, index.getArtifact("id1"));
  }

  @Test
  public void testCommitArtifact() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.commitArtifact(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID uuidId = null;
      expectedMessage = "Null UUID";
      index.commitArtifact(uuidId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertNull(index.commitArtifact("unknown"));
    assertNull(index.commitArtifact(UUID.randomUUID()));

    index.indexArtifact(artifact1);
    index.indexArtifact(artifact2);

    assertFalse(index.getArtifact("id1").getCommitted());
    assertFalse(index.getArtifact(uuid).getCommitted());

    index.commitArtifact("id1");

    assertTrue(index.getArtifact("id1").getCommitted());
    assertFalse(index.getArtifact(uuid).getCommitted());

    index.commitArtifact(uuid);

    assertTrue(index.getArtifact("id1").getCommitted());
    assertTrue(index.getArtifact(uuid).getCommitted());

    index.commitArtifact("id1");

    assertTrue(index.getArtifact("id1").getCommitted());
    assertTrue(index.getArtifact(uuid).getCommitted());
  }

  @Test
  public void testDeleteArtifact() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.deleteArtifact(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID uuidId = null;
      expectedMessage = "Null UUID";
      index.deleteArtifact(uuidId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertFalse(index.deleteArtifact("unknown"));
    assertFalse(index.deleteArtifact(UUID.randomUUID()));

    assertNull(index.getArtifact("id1"));
    assertNull(index.getArtifact(uuid.toString()));

    index.indexArtifact(artifact1);

    assertEquals("id1", index.getArtifact("id1").getId());
    assertNull(index.getArtifact(uuid.toString()));

    index.indexArtifact(artifact2);

    assertEquals("id1", index.getArtifact("id1").getId());
    assertEquals(uuid.toString(),
	index.getArtifact(uuid.toString()).getId());

    assertTrue(index.deleteArtifact("id1"));

    assertNull(index.getArtifact("id1"));
    assertEquals(uuid.toString(),
	index.getArtifact(uuid.toString()).getId());

    assertTrue(index.deleteArtifact(uuid));

    assertNull(index.getArtifact("id1"));
    assertNull(index.getArtifact(uuid.toString()));

    assertFalse(index.deleteArtifact(uuid));

    assertNull(index.getArtifact("id1"));
    assertNull(index.getArtifact(uuid.toString()));
  }

  @Test
  public void testArtifactExists() {
    String expectedMessage = "Null or empty identifier";

    try {
      String stringId = null;
      index.artifactExists(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertFalse(index.artifactExists("unknown"));

    assertFalse(index.artifactExists("id1"));
    assertFalse(index.artifactExists(uuid.toString()));

    index.indexArtifact(artifact1);

    assertTrue(index.artifactExists("id1"));
    assertFalse(index.artifactExists(uuid.toString()));

    index.indexArtifact(artifact2);

    assertTrue(index.artifactExists("id1"));
    assertTrue(index.artifactExists(uuid.toString()));
  }

  @Test
  public void testGetCollectionIds() {
    assertFalse(index.getCollectionIds().hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getCollectionIds().hasNext());

    index.commitArtifact("id1");

    Iterator<String> iter = index.getCollectionIds();
    assertTrue(iter.hasNext());
    assertEquals("coll1", iter.next());
    assertFalse(iter.hasNext());

    index.indexArtifact(artifact2);

    iter = index.getCollectionIds();
    assertTrue(iter.hasNext());
    assertEquals("coll1", iter.next());
    assertFalse(iter.hasNext());
    
    index.commitArtifact(uuid.toString());

    iter = index.getCollectionIds();
    assertTrue(iter.hasNext());

    Set<String> collections = new HashSet<>();
    collections.add(iter.next());
    assertTrue(iter.hasNext());
    collections.add(iter.next());
    assertFalse(iter.hasNext());
    assertEquals(2, collections.size());
    assertTrue(collections.contains("coll1"));
    assertTrue(collections.contains("coll2"));
  }

  @Test
  public void testGetAuIds() throws IOException {
    assertFalse(index.getAuIds(null).hasNext());
    assertFalse(index.getAuIds("coll1").hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getAuIds("coll1").hasNext());

    index.commitArtifact("id1");
    assertTrue(index.getAuIds("coll1").hasNext());

    Iterator<String> auIds = index.getAuIds("coll1");
    assertEquals("auid1", auIds.next());
    assertFalse(auIds.hasNext());

    assertFalse(index.getAuIds("coll2").hasNext());

    index.indexArtifact(artifact2);
    assertFalse(index.getAuIds("coll2").hasNext());

    index.commitArtifact(uuid.toString());
    assertTrue(index.getAuIds("coll2").hasNext());

    auIds = index.getAuIds("coll2");
    assertEquals("auid2", auIds.next());
    assertFalse(auIds.hasNext());
  }

  @Test
  public void testGetArtifactsInAU() {
    assertFalse(index.getAllArtifactsAllVersions(null, null).hasNext());
    assertFalse(index.getAllArtifactsAllVersions("coll1", null).hasNext());
    assertFalse(index.getAllArtifactsAllVersions("coll1", "auid1").hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getAllArtifactsAllVersions("coll1", "auid1").hasNext());

    index.commitArtifact("id1");

    Iterator<Artifact> iter = index.getAllArtifactsAllVersions("coll1", "auid1");
    assertTrue(iter.hasNext());
    Artifact aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());

    assertFalse(index.getAllArtifactsAllVersions("coll2", null).hasNext());
    assertFalse(index.getAllArtifactsAllVersions("coll2", "auid1").hasNext());
    assertFalse(index.getAllArtifactsAllVersions("coll2", "auid2").hasNext());

    index.indexArtifact(artifact2);
    assertFalse(index.getAllArtifactsAllVersions("coll2", "auid1").hasNext());
    assertFalse(index.getAllArtifactsAllVersions("coll2", "auid2").hasNext());

    index.commitArtifact(uuid.toString());
    assertFalse(index.getAllArtifactsAllVersions("coll2", "auid1").hasNext());

    iter = index.getAllArtifactsAllVersions("coll2", "auid2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
  }

  @Test
  public void getAllArtifactsWithPrefixAllVersions() {
    // Empty index
    assertFalse(index.getAllArtifactsWithPrefixAllVersions(null, null, null).hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", null, null).hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", null).hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1").hasNext());

    // Index artifact1
    index.indexArtifact(artifact1);
    
    // Before committing artifact1
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1").hasNext());

    // Commit artifact1
    index.commitArtifact("id1");

    // After committing artifact1
    
    // Prefix "uri" yields "uri1"
    Iterator<Artifact> iter1 = index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri");
    assertTrue(iter1.hasNext());
    Artifact art1 = iter1.next();
    assertEquals("id1", art1.getId());
    assertEquals("coll1", art1.getCollection());
    assertEquals("auid1", art1.getAuid());
    assertEquals("uri1", art1.getUri());
    assertFalse(iter1.hasNext());
    
    // Prefix "uri1" yields "uri1"
    Iterator<Artifact> iter2 = index.getAllArtifactsWithPrefixAllVersions("coll1", "auid1", "uri1");
    assertTrue(iter2.hasNext());
    Artifact art2 = iter2.next();
    assertEquals("id1", art2.getId());
    assertEquals("coll1", art2.getCollection());
    assertEquals("auid1", art2.getAuid());
    assertEquals("uri1", art2.getUri());
    assertFalse(iter2.hasNext());

    // Failed retrievals
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", null, null).hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", null).hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2").hasNext());

    // Index artifact2
    index.indexArtifact(artifact2);
    
    // Before committing artifact2
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri2").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2").hasNext());

    // Commit artifact2
    index.commitArtifact(uuid.toString());

    // After committing artifact2
    
    // Failed retrievals
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid1", "uri2").hasNext());
    assertFalse(index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri1").hasNext());

    // Prefix "uri" yields "uri2"
    Iterator<Artifact> iter3 = index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri");
    assertTrue(iter3.hasNext());
    Artifact art3 = iter3.next();
    assertEquals(uuid.toString(), art3.getId());
    assertEquals("coll2", art3.getCollection());
    assertEquals("auid2", art3.getAuid());
    assertEquals("uri2", art3.getUri());
    assertFalse(iter3.hasNext());

    // Prefix "uri2" yields "uri2"
    Iterator<Artifact> iter4 = index.getAllArtifactsWithPrefixAllVersions("coll2", "auid2", "uri2");
    assertTrue(iter4.hasNext());
    Artifact art4 = iter4.next();
    assertEquals(uuid.toString(), art4.getId());
    assertEquals("coll2", art4.getCollection());
    assertEquals("auid2", art4.getAuid());
    assertEquals("uri2", art4.getUri());
    assertFalse(iter4.hasNext());
  }

  @Test
  public void getArtifactAllVersions() {
    // Empty index
    assertFalse(index.getArtifactAllVersions(null, null, null).hasNext());
    assertFalse(index.getArtifactAllVersions("coll1", null, null).hasNext());
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", null).hasNext());
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", "uri1").hasNext());

    // Index artifact1
    index.indexArtifact(artifact1);
    
    // Before committing artfact1
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", "uri1").hasNext());

    // Commit artifact1
    index.commitArtifact("id1");

    // After committing artifact1
    assertFalse(index.getArtifactAllVersions("coll1", "auid1", "uri").hasNext());

    Iterator<Artifact> iter1 = index.getArtifactAllVersions("coll1", "auid1", "uri1");
    assertTrue(iter1.hasNext());
    Artifact art1 = iter1.next();
    assertEquals("id1", art1.getId());
    assertEquals("coll1", art1.getCollection());
    assertEquals("auid1", art1.getAuid());
    assertEquals("uri1", art1.getUri());
    assertFalse(iter1.hasNext());

    // Before indexing artifact2
    assertFalse(index.getArtifactAllVersions("coll2", null, null).hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", null).hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri1").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri2").hasNext());

    // Index artifact2
    index.indexArtifact(artifact2);

    // Before committing artifact2
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri2").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri1").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri2").hasNext());

    // Commit artifact2
    index.commitArtifact(uuid.toString());

    // After committing artifact2
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri1").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid1", "uri2").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri").hasNext());
    assertFalse(index.getArtifactAllVersions("coll2", "auid2", "uri1").hasNext());

    Iterator<Artifact> iter2 = index.getArtifactAllVersions("coll2", "auid2", "uri2");
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

  @Test
  public void testQuery() {
    ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertFalse(index.query(query).hasNext());

    index.indexArtifact(artifact1);
    query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertTrue(index.query(query).hasNext());

    query.filterByAuid("auid1");
    assertTrue(index.query(query).hasNext());

    query.filterByCollection("coll1");
    assertTrue(index.query(query).hasNext());

    query.filterByURIPrefix("uri");
    assertTrue(index.query(query).hasNext());

    query.filterByURIPrefix("uri1");
    assertTrue(index.query(query).hasNext());

    query.filterByURIMatch("uri1");
    assertTrue(index.query(query).hasNext());

    query.filterByVersion(1);
    assertTrue(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByAuid(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByAuid("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCollection(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCollection("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIPrefix(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIPrefix("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIMatch(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIMatch("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByVersion(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByVersion(123);
    assertFalse(index.query(query).hasNext());

    index.commitArtifact(artifact1.getIdentifier().getId());
    query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertTrue(index.query(query).hasNext());
  }
}
