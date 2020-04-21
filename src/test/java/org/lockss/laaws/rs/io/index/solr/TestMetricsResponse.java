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

package org.lockss.laaws.rs.io.index.solr;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.test.LockssTestCase5;

public class TestMetricsResponse extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  @Disabled
  @Test
  public void test() throws Exception {
    // Get SolrClient from Solr REST endpoint
    HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl("http://localhost:8983/solr")
        .build();

    MetricsRequest.CoreMetricsRequest req = new MetricsRequest.CoreMetricsRequest();
    MetricsResponse.CoreMetricsResponse metrics = req.process(solrClient);

    log.trace("metrics = {}", metrics);
    log.trace("metrics.getTotalSpace = {}", metrics.getTotalSpace());
    log.trace("metrics.getTotalSpace = {}", metrics.getIndexSizeInBytes());

    SolrArtifactIndex index = new SolrArtifactIndex(solrClient);
    StorageInfo info = index.getStorageInfo();

    log.trace("StorageInfo = {}", info);
  }
}
