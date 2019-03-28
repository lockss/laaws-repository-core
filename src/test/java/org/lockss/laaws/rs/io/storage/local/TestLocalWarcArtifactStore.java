/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;

public class TestLocalWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<LocalWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  private File testsRootPath;
  private File testRepoBasePath;

  @BeforeAll
  protected void makeLocalTempDir() throws IOException {
    File tempFile = File.createTempFile(getClass().getSimpleName(), null);
    tempFile.deleteOnExit();

    testsRootPath = new File(tempFile.getAbsolutePath() + ".d");
    testsRootPath.mkdirs();
  }

  @AfterAll
  public void tearDown() {
    quietlyDeleteDir(testsRootPath);
  }

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    testRepoBasePath = new File(testsRootPath + "/" + UUID.randomUUID());
    testRepoBasePath.mkdirs();
    return new LocalWarcArtifactDataStore(index, testRepoBasePath);
  }

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, LocalWarcArtifactDataStore other) throws IOException {
    return new LocalWarcArtifactDataStore(index, other.getBasePath());
  }

  @Test
  public void testInitCollection() throws Exception {
    store.initCollection("collection");
    assertTrue(isDirectory(store.getCollectionPath("collection")));
  }

  @Test
  public void testInitAu() throws Exception {
    store.initAu("collection", "auid");
    assertTrue(isDirectory(store.getCollectionPath("collection")));
    assertTrue(isDirectory(store.getAuPath("collection", "auid")));
  }

  @Override
  protected boolean isValidStorageUrl(String storageUrl) {
    return true;
  }

  protected static void quietlyDeleteDir(File dir) {
    try {
      FileUtils.deleteDirectory(dir);
    } catch (IOException e) {
      // oh well.
    }
  }

  @Override
  protected boolean pathExists(String path) throws IOException {
    File pathDir = new File(path);
    return pathDir.exists();
  }

  @Override
  protected boolean isDirectory(String path) throws IOException {
    File pathDir = new File(path);
    return pathDir.isDirectory();
  }

  @Override
  protected boolean isFile(String path) throws IOException {
    File pathDir = new File(path);
    return pathDir.isFile();
  }

  @Override
  protected String expected_getTmpWarcBasePath() {
    return store.getAbsolutePath("/temp");
  }

  protected String expected_makeStorageUrl(ArtifactIdentifier aid, long offset, long length) throws Exception {
    return String.format(
        "file://%s?offset=%d&length=%d",
        store.getActiveWarcPath(aid),
        offset,
        length
    );
  }
}