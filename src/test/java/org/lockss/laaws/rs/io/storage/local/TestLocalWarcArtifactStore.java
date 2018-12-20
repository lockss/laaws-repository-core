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
import java.util.UUID;

import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.storage.warc.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;

public class TestLocalWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<LocalWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();
  private File testsRootDir;

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore() throws IOException {
    File testRepoBaseDir = new File(testsRootDir + "/" + UUID.randomUUID());
    testRepoBaseDir.mkdirs();
    return new LocalWarcArtifactDataStore(testRepoBaseDir);
  }

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(LocalWarcArtifactDataStore other) throws IOException {
    return new LocalWarcArtifactDataStore(other.getBasePath());
  }

  @BeforeAll
  protected void makeLocalTempDir() throws IOException {
    File tempFile = File.createTempFile(getClass().getSimpleName(), null);
    tempFile.deleteOnExit();

    testsRootDir = new File(tempFile.getAbsolutePath() + ".d");
    testsRootDir.mkdirs();
  }

  @AfterAll
  public void tearDown() {
    quietlyDeleteDir(testsRootDir);
  }

  @Override
  protected boolean pathExists(String path) throws IOException {
    File pathDir = new File(store.getBasePath(), path);
    return pathDir.exists();
  }

  @Override
  protected boolean isDirectory(String path) throws IOException {
    File pathDir = new File(store.getBasePath(), path);
    return pathDir.isDirectory();
  }

  @Override
  protected boolean isFile(String path) throws IOException {
    File pathDir = new File(store.getBasePath(), path);
    return pathDir.isFile();
  }

//  @Test
//  public void testMkdirsIfNeeded() throws Exception {
//    File tmp1 = makeTempDir();
//    String dirPath = tmp1.getAbsolutePath() + "/foo/bar/baz";
//    File dir = new File(dirPath);
//    assertFalse(dir.exists());
//    LocalWarcArtifactDataStore.mkdirs(dirPath);
//    assertTrue(dir.isDirectory());
//    LocalWarcArtifactDataStore.mkdirs(dirPath); // should not fail or throw
//    quietlyDeleteDir(tmp1);
//  }

  @Override
  protected String testMakeStorageUrl_getExpected(ArtifactIdentifier ident,
                                                  long offset)
      throws Exception {
    return String.format("file://%s%s?offset=%d",
                         (store.getBasePath().equals("/") ? "" : store.getBasePath()),
                         store.getActiveWarcPath(ident),
                         offset);
  }

  @Override
  protected Artifact testMakeNewStorageUrl_makeArtifactNotNeedingUrl(ArtifactIdentifier ident)
      throws Exception {
    Artifact art = new Artifact(ident,
                                Boolean.TRUE,
                                String.format("file://%s%s/%s?offset=1234",
                                              store.getBasePath(),
                                              store.getSealedWarcsPath(),
                                              store.getSealedWarcName(ident.getCollection(), ident.getAuid())),
                                123L,
                                "0x12345");
    return art;
  }
  
  @Override
  protected Artifact testMakeNewStorageUrl_makeArtifactNeedingUrl(ArtifactIdentifier ident)
      throws Exception {
    Artifact art = new Artifact(ident,
                                Boolean.TRUE,
                                String.format("file://%s?offset=1234",
                                              store.getActiveWarcPath(ident)),
                                123L,
                                "0x12345");
    return art;
  }
  
  @Override
  protected void testMakeNewStorageUrl_checkArtifactNeedingUrl(Artifact artifact,
                                                               String newPath,
                                                               String result)
      throws Exception {
    assertThat(result, startsWith(String.format("file://" + newPath)));
  }
  

  
}