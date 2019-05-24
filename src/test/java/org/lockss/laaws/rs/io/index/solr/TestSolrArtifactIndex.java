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

import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.log.L4JLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class TestSolrArtifactIndex extends AbstractArtifactIndexTest<SolrArtifactIndex> {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static MiniSolrCloudCluster cluster;
  private static CloudSolrClient client;
  private String collectionName;

  @BeforeAll
  protected static void startMiniSolrCloudCluster() throws IOException {
    // Base directory for the MiniSolrCloudCluster
    Path tempDir = Files.createTempDirectory("MiniSolrCloudCluster");

    // Jetty configuration
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);

    try {
      // Start new MiniSolrCloudCluster with default solr.xml
      cluster = new MiniSolrCloudCluster(1, tempDir, jettyConfig.build());

      // Upload our Solr configuration set for tests
      cluster.uploadConfigSet(new File("src/test/resources/solr/configsets/lockss-solrtest/conf").getAbsoluteFile().toPath(), "lockss-solrtest");

      // Get a Solr client handle to the Solr Cloud cluster
      client = cluster.getSolrClient();
      client.connect();
    } catch (Exception e) {
      log.error("Could not start MiniSolrCloudCluster", e);
    }
  }

  //@BeforeEach
  @Override
  protected SolrArtifactIndex makeArtifactIndex() throws IOException {
    // New collection name for this test
    collectionName = String.format("lockss-solrtest.%s", UUID.randomUUID());

    log.debug("collectionName = {}", collectionName);

    try {
      // Create a new Solr collection
      CollectionAdminRequest
          .createCollection(collectionName, "lockss-solrtest", 1, 1)
          .processAndWait(client, 30);

      // Set default collection
      client.setDefaultCollection(collectionName);

    } catch (Exception e) {
      log.error("Could not create temporary Solr collection [collectionName: {}]:", collectionName, e);
      throw new IOException(e); // FIXME
    }

    return new SolrArtifactIndex(client);
  }

  @AfterEach
  public void removeSolrCollection() throws Exception {
    // Get a Solr client handle to the Solr Cloud cluster
    CloudSolrClient client = cluster.getSolrClient();
    client.connect();

    CollectionAdminRequest.deleteCollection(collectionName).processAndWait(client, 30);
  }
}