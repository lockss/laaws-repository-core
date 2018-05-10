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

package org.lockss.laaws.rs.io.storage.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.storage.warc.AbstractWarcArtifactDataStoreTest;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.util.FileUtil;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigBuilder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class TestHdfsWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<HdfsWarcArtifactDataStore> {
    private final static Log log = LogFactory.getLog(TestHdfsWarcArtifactStore.class);

    @Override
    protected HdfsWarcArtifactDataStore makeWarcArtifactDataStore(String repoBasePath) throws IOException {
        try {
//            System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);

//            Configuration conf = new Configuration();
            Configuration conf = new HdfsConfiguration();
            File baseDir = new File("target/test-hdfs/" + UUID.randomUUID());
            baseDir.mkdirs();
            assertTrue(baseDir.exists() && baseDir.isDirectory());

            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

            log.info(String.format("WOLF4 %s: %s, getBaseDirectory(): %s",
                    MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
                    conf.get(MiniDFSCluster.HDFS_MINIDFS_BASEDIR),
                    MiniDFSCluster.getBaseDirectory()
            ));

            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
//            builder.numDataNodes(3);
//            builder.clusterId("test");
            assertNotNull(builder);

            MiniDFSCluster hdfsCluster = builder.build();

            String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";

            return new HdfsWarcArtifactDataStore(hdfsCluster.getFileSystem(), new Path("/"));
        } catch (Exception e) {
            //throw new IOException(e);
            throw e;
        }
    }

    @Override
    protected boolean pathExists(String path) throws IOException {
        Path hdfsPath = new Path(store.getBasePath(), path);
        return store.fs.exists(hdfsPath);
    }

    @Override
    protected boolean isDirectory(String path) throws IOException {
        return store.fs.isDirectory(new Path(store.getBasePath(), path));
    }

    @Override
    protected boolean isFile(String path) throws IOException {
        Path file = new Path(store.getBasePath(), path);
        log.info(String.format("Checking whether %s is a file in HDFS", file));

        if (!store.fs.exists(file)) {
            String errMsg = String.format("%s does not exist!", file);
            log.warn(errMsg);
        }

        return store.fs.isFile(new Path(store.getBasePath(), path));
    }

    @Override
    protected String testMakeStorageUrl_getExpected(ArtifactIdentifier ident, long offset) throws Exception {
        return String.format("%s%s%s?offset=%d",
//                store.getBasePath(),
                store.fs.getUri(),
                (store.getBasePath().equals("/") ? "" : store.getBasePath()),
                store.getAuArtifactsWarcPath(ident),
                offset);
    }

    @Override
    protected Artifact testMakeNewStorageUrl_makeArtifactNotNeedingUrl(WarcArtifactDataStore store, ArtifactIdentifier ident) throws Exception {
        Artifact art = new Artifact(ident,
                Boolean.TRUE,
                String.format("hdfs://%s%s/%s?offset=1234",
                        store.getBasePath(),
                        store.getSealedWarcPath(),
                        store.getSealedWarcName(ident.getCollection(), ident.getAuid())),
                123L,
                "0x12345");
        return art;
    }

    @Override
    protected Artifact testMakeNewStorageUrl_makeArtifactNeedingUrl(WarcArtifactDataStore store, ArtifactIdentifier ident) throws Exception {
        Artifact art = new Artifact(ident,
                Boolean.TRUE,
                String.format("hdfs://%s?offset=1234",
                        store.getAuArtifactsWarcPath(ident)),
                123L,
                "0x12345");
        return art;
    }

    @Override
    protected void testMakeNewStorageUrl_checkArtifactNeedingUrl(WarcArtifactDataStore store, Artifact artifact, String newPath, String result) throws Exception {
        assertThat(result, startsWith(String.format("hdfs://" + newPath)));
    }
}