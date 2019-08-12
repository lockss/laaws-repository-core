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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestSolrArtifactIndex extends AbstractArtifactIndexTest<SolrArtifactIndex> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // TODO: Externalize this path / make it configurable elsewhere:
  private static final Path SOLR_BASE_PATH = Paths.get("target/test-classes/solr");
  private static final String SOLR_TEST_CORE_NAME = "testcore";
  private static final String SOLR_TEST_CORE_DIR = "testcore";

  private static SolrClient client;
  private static File tmpSolrHome;

  // *******************************************************************************************************************
  // * JUNIT LIFECYCLE
  // *******************************************************************************************************************

  @BeforeAll
  protected static void startEmbeddedSolrServer() throws IOException {
    // Make a copy of the test Solr home environment
    tmpSolrHome = FileUtil.createTempDir("testSolrHome", null);
    FileUtils.copyDirectory(SOLR_BASE_PATH.toFile(), tmpSolrHome);

    // Start EmbeddedSolrServer pointing to the temporary
    client = new EmbeddedSolrServer(tmpSolrHome.toPath(), SOLR_TEST_CORE_NAME);
  }

  @AfterAll
  protected static void shutdownEmbeddedSolrServer() throws Exception {
    // Shutdown the EmbeddedSolrServer
    client.close();

    // Remove temporary Solr home copy
    FileUtils.deleteQuietly(tmpSolrHome);
  }

  @Override
  protected SolrArtifactIndex makeArtifactIndex() throws Exception {
    CoreAdminRequest.createCore(SOLR_TEST_CORE_NAME, SOLR_TEST_CORE_DIR, client);
    return new SolrArtifactIndex(client);
  }

  @AfterEach
  public void removeSolrCollection() throws Exception {
    CoreAdminRequest.unloadCore(SOLR_TEST_CORE_NAME, true, client);
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
}