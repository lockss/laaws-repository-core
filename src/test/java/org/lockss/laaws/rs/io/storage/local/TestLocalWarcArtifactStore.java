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

package org.lockss.laaws.rs.io.storage.local;

import java.io.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.*;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.*;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.index.*;
import org.lockss.laaws.rs.io.storage.warc.*;
import org.lockss.laaws.rs.model.*;

public class TestLocalWarcArtifactStore extends AbstractWarcArtifactDataStoreTest {

  @Override
  protected WarcArtifactDataStore makeWarcArtifactDataStore(String repoBasePath) throws IOException {
    return new LocalWarcArtifactDataStore(repoBasePath);
  }
  
  @BeforeEach
  public void setUp() throws Exception {
    
  }
  
  @Test
  public void rebuildIndex() {
  }

  @Test
  public void rebuildIndex1() {
  }

  @Test
  public void scanDirectories() {
  }

  @Test
  public void getArchicalUnitBasePath() {
  }

  @Test
  public void getCollectionBasePath() {
  }

  @Test
  public void mkdirIfNotExist() {
  }

  @Test
  public void addArtifact() {
  }

  @Test
  public void getArtifact() {
  }

  @Test
  public void updateArtifactMetadata() {
  }

  @Test
  public void commitArtifact() {
  }

  @Test
  public void isDeleted() {
  }

  @Test
  public void isCommitted() {
  }

  @Test
  public void deleteArtifact() {
  }

  @Test
  public void getWarcRecordId() {
  }

  @Test
  public void getWarcRecord() {
  }

  @Test
  public void createWarcMetadataRecord() {
  }

  @Test
  public void writeArtifact() {
  }

  @Test
  public void writeWarcRecord() {
  }

  @Test
  public void createRecordHeader() {
  }

  @Test
  public void createWARCInfoRecord() {
  }

//  @Test
//  public void testMkdirsIfNeeded() throws Exception {
//    File tmp1 = makeTempDir();
//    String dirPath = tmp1.getAbsolutePath() + "/foo/bar/baz";
//    File dir = new File(dirPath);
//    assertFalse(dir.exists());
//    LocalWarcArtifactDataStore.mkdirsIfNeeded(dirPath);
//    assertTrue(dir.isDirectory());
//    LocalWarcArtifactDataStore.mkdirsIfNeeded(dirPath); // should not fail or throw
//    quietlyDeleteDir(tmp1);
//  }

  @Override
  protected String testMakeStorageUrl_getExpected(WarcArtifactDataStore store,
                                                  ArtifactIdentifier ident,
                                                  long offset)
      throws Exception {
    return String.format("file://%s%s?offset=%d",
                         store.getBasePath(),
                         store.getAuArtifactsWarcPath(ident),
                         offset);
  }

  @Override
  protected Artifact testMakeNewStorageUrl_makeArtifactNotNeedingUrl(WarcArtifactDataStore store,
                                                                     ArtifactIdentifier ident)
      throws Exception {
    Artifact art = new Artifact(ident,
                                Boolean.TRUE,
                                String.format("file://%s%s/%s?offset=1234",
                                              store.getBasePath(),
                                              store.getSealedWarcPath(),
                                              store.getSealedWarcName(ident.getCollection(), ident.getAuid())),
                                123L,
                                "0x12345");
    return art;
  }
  
  @Override
  protected Artifact testMakeNewStorageUrl_makeArtifactNeedingUrl(WarcArtifactDataStore store,
                                                                  ArtifactIdentifier ident)
      throws Exception {
    Artifact art = new Artifact(ident,
                                Boolean.TRUE,
                                String.format("file://%s?offset=1234",
                                              store.getAuArtifactsWarcPath(ident)),
                                123L,
                                "0x12345");
    return art;
  }
  
  @Override
  protected void testMakeNewStorageUrl_checkArtifactNeedingUrl(WarcArtifactDataStore store,
                                                               Artifact artifact,
                                                               String newPath,
                                                               String result)
      throws Exception {
    assertThat(result, startsWith(String.format("file://" + newPath)));
  }
  
  @Test
  public void testWarcSealing() throws Exception {
    File tmp1 = makeTempDir();
    ArtifactIndex index = new VolatileArtifactIndex();
    LocalWarcArtifactDataStore store = new LocalWarcArtifactDataStore(tmp1.getAbsolutePath());
    store.setArtifactIndex(index);
    
    // The WARC records for the two artifacts here end up being 586 bytes each.
    store.setThresholdWarcSize(1024L);

    StatusLine status200 = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");
    
    File auDir = new File(store.getBasePath() + "/collections/coll1/au-" + DigestUtils.md5Hex("auid1"));
    File auArtifactsWarc = new File(auDir, "artifacts.warc");
    File auMetadataWarc = new File(auDir, "lockss-repo.warc");
    File sealedWarcDir = new File(store.getBasePath() + "/sealed");
    
    assertFalse(auDir.exists());
    assertFalse(auArtifactsWarc.exists());
    assertFalse(auMetadataWarc.exists());
    assertTrue(sealedWarcDir.isDirectory());
    
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);
    ByteArrayOutputStream baos1 = new ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos1.write('a');
    }
    ArtifactData dat1 = new ArtifactData(ident1, null, baos1.toInputStream(), status200);
    Artifact art1 = store.addArtifactData(dat1);
    baos1.close(); // to satisfy static analyzers

    index.indexArtifact(dat1);
    index.commitArtifact(art1.getId());
    assertNotNull(index.getArtifact(art1.getId()));
    
    assertTrue(auDir.isDirectory());
    assertTrue(auArtifactsWarc.isFile());
    assertTrue(auMetadataWarc.isFile());
    assertTrue(sealedWarcDir.isDirectory());
    assertEquals(dat1.getStorageUrl(), art1.getStorageUrl());
    assertThat(art1.getStorageUrl(), startsWith("file://" + auArtifactsWarc.getAbsolutePath()));
    
    ArtifactIdentifier ident2 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u2", 1);
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos2.write('b');
    }
    ArtifactData dat2 = new ArtifactData(ident2, null, baos2.toInputStream(), status200);
    Artifact art2 = store.addArtifactData(dat2);
    baos2.close(); // to satisfy static analyzers

    index.indexArtifact(dat2);
    index.commitArtifact(art2.getId());
    assertNotNull(index.getArtifact(art2.getId()));
    
    assertTrue(auDir.isDirectory());
    assertFalse(auArtifactsWarc.exists());
    assertTrue(auMetadataWarc.isFile());
    assertTrue(sealedWarcDir.isDirectory());
    assertEquals(dat2.getStorageUrl(), art2.getStorageUrl());
    assertThat(art2.getStorageUrl(), startsWith("file://" + sealedWarcDir.getAbsolutePath()));

    Artifact art1i = index.getArtifact(art1.getId());
    assertNotNull(art1i);
    assertNotEquals(art1.getStorageUrl(), art1i.getStorageUrl());
    assertThat(art1i.getStorageUrl(), startsWith("file://" + sealedWarcDir.getAbsolutePath()));
    
    quietlyDeleteDir(tmp1);
  }
  
}