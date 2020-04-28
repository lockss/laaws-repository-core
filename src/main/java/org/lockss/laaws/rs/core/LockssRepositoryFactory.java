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

package org.lockss.laaws.rs.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrClient;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.hdfs.HdfsWarcArtifactDataStore;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Factory for common LOCKSS Repository configurations.
 */
public class LockssRepositoryFactory {
    /**
     * Instantiates a LOCKSS repository in memory. Not intended for production use.
     *
     * @return A {@code VolatileLockssRepository} instance.
     */
    public static LockssRepository createVolatileRepository() throws IOException {
        return new VolatileLockssRepository();
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository. Uses a volatile index that must be rebuilt upon each
     * instantiation. Use of a volatile index is not recommended for large installations.
     *
     * @param basePath
     *          A {@code File} containing the base path of this LOCKSS Repository.
     * @param persistedIndexName
     *          A String with the name of the file where to persist the index.
     * @return A {@code LocalLockssRepository} instance.
     */
    public static LockssRepository createLocalRepository(File basePath, String persistedIndexName) throws IOException {
        return new LocalLockssRepository(basePath, persistedIndexName);
    }

    /**
     * Instantiates a LOCKSS repository backed by a local data store with one or more base paths, and a locally
     * persisting artifact index.
     *
     * @param basePaths          A {@link File[]} containing the base paths of the local data store.
     * @param persistedIndexName A {@link String} containing the locally persisted artifact index name.
     * @return A {@link LocalLockssRepository} backed by a local data store and locally persisted artifact index.
     * @throws IOException
     */
    public static LockssRepository createLocalRepository(File[] basePaths, String persistedIndexName) throws IOException {
        return new LocalLockssRepository(basePaths, persistedIndexName);
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository with a provided ArtifactIndex implementation for artifact
     * indexing. It does not invoke rebuilding the index, so it is only appropriate for implementations that persist.
     *
     * @param basePath A {@code File} containing the base path of this LOCKSS Repository.
     * @param index    An {@code ArtifactIndex} to use as this repository's artifact index.
     * @return A {@code LocalLockssRepository} instance.
     */
    public static LockssRepository createLocalRepository(File basePath, ArtifactIndex index) throws IOException {
        return new LocalLockssRepository(index, basePath);
    }

    /**
     * Instantiates a LOCKSS repository backed by a local data store with one or more base paths, and the provided
     * artifact index.
     *
     * @param basePaths A {@link File[]} containing the base paths of the local data store.
     * @param index     An {@link ArtifactIndex} to use as this repository's artifact index.
     * @return A {@link LocalLockssRepository} backed by a local data store and locally persisted artifact index.
     * @throws IOException
     */
    public static LockssRepository createLocalRepository(File[] basePaths, ArtifactIndex index) throws IOException {
        return new LocalLockssRepository(index, basePaths);
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
    public static LockssRepository createLargeLockssRepository(
        String coreName,
        SolrClient solrClient,
        Configuration hadoopConf,
        String basePath
    ) throws IOException {
        ArtifactIndex index = new SolrArtifactIndex(coreName, solrClient);
        ArtifactDataStore store = new HdfsWarcArtifactDataStore(index, hadoopConf, Paths.get(basePath));
        return new BaseLockssRepository(index, store);
    }

    /**
     * Instantiates a LOCKSS repository proxy to a remote LOCKSS Repository
     * Service over REST.
     *
     * @param repositoryServiceUrl Base {@code URL} of the remote LOCKSS
     *                             Repository service.
     * @param userName             A String with the name of the user used to
     *                             access the remote LOCKSS Repository service.
     * @param password             A String with the password of the user used
     *                             to access the remote LOCKSS Repository
     *                             service.
     * @return A {@code RestLockssRepository}.
     */
    public static RestLockssRepository createRestLockssRepository(
	URL repositoryServiceUrl, String userName, String password) {
        return new RestLockssRepository(repositoryServiceUrl, userName,
            password);
    }

}
