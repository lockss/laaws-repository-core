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

package org.lockss.laaws.rs.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.hdfs.HdfsWarcArtifactStore;

import java.io.File;
import java.net.URL;

/**
 * Factory for common LOCKSS Repository configurations.
 */
public class LockssRepositoryFactory {
    /**
     * Instantiates a LOCKSS repository in memory. Not intended for production use.
     *
     * @return A {@code VolatileLockssRepository} instance.
     */
    public static LockssRepository createVolatileRepository() {
        return new VolatileLockssRepository();
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository. Uses a volatile index that must be rebuilt upon each
     * instantiation. Use of a volatile index is not recommended for large installations.
     *
     * @param basePath
     *          A {@code File} containing the base path of this LOCKSS Repository.
     * @return A {@code LocalLockssRepository} instance.
     */
    public static LockssRepository createLocalRepository(File basePath) {
        return new LocalLockssRepository(basePath);
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository with a provided ArtifactIndex implementation for artifact
     * indexing. It does not invoke rebuilding the index, so it is only appropriate for implementations that persist.
     *
     * @param basePath
     *          A {@code File} containing the base path of this LOCKSS Repository.
     * @param index
     *          An {@code ArtifactIndex} to use as this repository's artifact index.
     * @return A {@code LocalLockssRepository} instance.
     */
    public static LockssRepository createLocalRepository(File basePath, ArtifactIndex index) {
        return new LocalLockssRepository(basePath, index);
    }

    /**
     * Instantiates a LOCKSS Repository that uses an Apache Hadoop Distributed Filesystem (HDFS) for storage, and a
     * Apache Solr index for the indexing of artifacts in the repository.
     *
     * @param solrClient
     *          A {@code SolrClient} pointing to the Apache Solr collection to use for artifact indexing.
     * @param hadoopConf
     *          An Apache Hadoop {@code Configuration} pointing to the HDFS cluster to use for artifact storage.
     * @param basePath
     *          A HDFS {@code Path} containing the base path under the HDFS cluster to use for the storage of artifacts.
     * @return A {@code BaseLockssRepository} instance configured to use Solr and HDFS.
     */
    public static LockssRepository createLargeLockssRepository(SolrClient solrClient, Configuration hadoopConf, Path basePath) {
        ArtifactIndex index = new SolrArtifactIndex(solrClient);
        ArtifactDataStore store = new HdfsWarcArtifactStore(hadoopConf, basePath);
        return new BaseLockssRepository(index, store);
    }

    /**
     * Instantiates a LOCKSS repository proxy to a remote LOCKSS Repository Service over REST.
     *
     * @param repositoryServiceUrl
     *          Base {@code URL} of the remote LOCKSS Repository service.
     * @return A {@code RestLockssRepository}.
     */
    public static LockssRepository createRestLockssRepository(URL repositoryServiceUrl) {
        return new RestLockssRepository(repositoryServiceUrl);
    }
}
