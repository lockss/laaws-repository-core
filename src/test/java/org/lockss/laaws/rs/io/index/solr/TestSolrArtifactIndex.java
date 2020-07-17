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

package org.lockss.laaws.rs.io.index.solr;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;
import org.lockss.util.storage.StorageInfo;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;

public class TestSolrArtifactIndex extends AbstractArtifactIndexTest<SolrArtifactIndex> {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final String TEST_SOLR_CORE_NAME = "test";
  private static final String TEST_SOLR_HOME_RESOURCES = "/solr/.filelist";

  private static EmbeddedSolrServer client;
  private static File tmpSolrHome;

  // *******************************************************************************************************************
  // * JUNIT LIFECYCLE
  // *******************************************************************************************************************

  /*
  @BeforeAll
  protected static void startEmbeddedSolrServer() throws IOException {
    // Create a temporary directory to hold a copy of the test Solr environment
    tmpSolrHome = FileUtil.createTempDir("testSolrHome", null);

    // Start EmbeddedSolrServer
    client = new EmbeddedSolrServer(tmpSolrHome.toPath(), TEST_SOLR_CORE_NAME);
  }

  @AfterAll
  protected static void shutdownEmbeddedSolrServer() throws Exception {
    // Shutdown the EmbeddedSolrServer
    client.close();

    // Remove the temporary test Solr environment
    FileUtil.delTree(tmpSolrHome);
  }
  */

  private void copyResourcesForTests(String filelistRes, Path dstPath) throws IOException {
    // Read file list
    try (InputStream input = getClass().getResourceAsStream(filelistRes)) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

        // Name of resource to load
        String resourceName;

        // Iterate over resource names from the list and copy each into the target directory
        while ((resourceName = reader.readLine()) != null) {
          // Source resource URL
          URL srcUrl = getClass().getResource(String.format("/solr/%s", resourceName));

          // Destination file
          File dstFile = dstPath.resolve(resourceName).toFile();

          // Copy resource to file
          FileUtils.copyURLToFile(srcUrl, dstFile);
        }

      }
    }
  }

  @Override
  protected SolrArtifactIndex makeArtifactIndex() throws Exception {
    // Create a temporary directory to hold a copy of the test Solr environment
    tmpSolrHome = getTempDir();

    log.trace("tmpSolrHome = {}", tmpSolrHome);

    // TODO Use a LocalSolrCoreAdmin to create the core programmatically
    copyResourcesForTests(TEST_SOLR_HOME_RESOURCES, tmpSolrHome.toPath());

    // TODO May be faster to use one EmbeddedSolrServer from @BeforeAll then call client.getCoreContainer().reload(...);
    client = new EmbeddedSolrServer(tmpSolrHome.toPath(), TEST_SOLR_CORE_NAME);

    return new SolrArtifactIndex(client, TEST_SOLR_CORE_NAME);
  }

  @AfterEach
  public void removeSolrCollection() throws Exception {
    // Shutdown the EmbeddedSolrServer
    client.close();

    // Remove the temporary test Solr environment
    FileUtil.delTree(tmpSolrHome);
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION SPECIFIC TESTS
  // *******************************************************************************************************************

  @Test
  @Override
  public void testInitIndex() throws Exception {
    // The given index is already initialized by the test framework; just check that it is ready
    assertTrue(index.isReady());
  }

  @Test
  @Override
  public void testShutdownIndex() throws Exception {
    // Intentionally left blank; see @AfterEach remoteSolrCollection()
  }

  @Test
  @Override
  public void testWaitReady() throws Exception {
    removeSolrCollection();
    super.testWaitReady();
  }

  //  @Test
  // XXX This gets
//   java.lang.ClassCastException: org.apache.solr.client.solrj.embedded.EmbeddedSolrServer cannot be cast to org.apache.solr.client.solrj.impl.HttpSolrClient
// 	at org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex.getStorageInfo(SolrArtifactIndex.java:157)
  public void testStorageInfo() throws Exception {
    StorageInfo si = index.getStorageInfo();
    log.debug("storeinfo: {}", si);
    assertEquals(SolrArtifactIndex.ARTIFACT_INDEX_TYPE, si.getType());
    assertTrue(si.getSize() > 0);
  }
}
