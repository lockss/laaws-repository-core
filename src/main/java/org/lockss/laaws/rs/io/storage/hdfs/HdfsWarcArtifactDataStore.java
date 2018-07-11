/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Apache Hadoop Distributed File System (HDFS) implementation of WarcArtifactDataStore.
 */
public class HdfsWarcArtifactDataStore extends WarcArtifactDataStore {
    private final static Log log = LogFactory.getLog(HdfsWarcArtifactDataStore.class);
    public final static String DEFAULT_REPO_BASEDIR = "/";

    protected FileSystem fs;

    public HdfsWarcArtifactDataStore(Configuration config) throws IOException {
        this(config, DEFAULT_REPO_BASEDIR);
    }

    /**
     * Constructor.
     *
     * @param config
     *          A Apache Hadoop {@code Configuration}
     * @param basePath
     *          A {@code String} to the base directory of the LOCKSS repository under HDFS
     */
    public HdfsWarcArtifactDataStore(Configuration config, String basePath) throws IOException {
        this(FileSystem.get(config), basePath);
    }

    public HdfsWarcArtifactDataStore(FileSystem fs) throws IOException {
        this(fs, DEFAULT_REPO_BASEDIR);
    }

    public HdfsWarcArtifactDataStore(FileSystem fs, String basePath) throws IOException {
        super(basePath);

        log.info(String.format(
                "Instantiating a HDFS artifact data store under %s%s",
                fs.getUri(),
                this.basePath
        ));

        this.fs = fs;
        this.fileAndOffsetStorageUrlPat =
            Pattern.compile("(" + fs.getUri() + ")(" + (getBasePath().equals("/") ? "" : getBasePath()) + ")([^?]+)\\?offset=(\\d+)");

        initializeLockssRepository();
    }

    /**
     * Initializes a LOCKSS repository structure under the configured HDFS base path.
     *
     * @throws IOException
     */
    public void initializeLockssRepository() throws IOException {
        mkdirsIfNeeded("/");
        mkdirsIfNeeded(getSealedWarcPath());
    }

    /**
     * Recursively finds artifact WARC files under a given base path.
     *
     * @param basePath The base path to scan recursively for WARC files.
     * @return A collection of paths to WARC files under the given base path.
     * @throws IOException
     */
    @Override
    public Collection<String> scanDirectories(String basePath) throws IOException {
        Collection<String> warcFiles = new ArrayList<>();

//        RemoteIterator<LocatedFileStatus> files = fs.listFiles(base, false);
//
//        while (files.hasNext()) {
//            LocatedFileStatus status = files.next();
//            if (status.isDirectory()) {
//                warcFiles.addAll(scanDirectories(status.getPath()));
//            } else {
//                if (status.isFile() && status.getPath().getName().toLowerCase().endsWith(WARC_FILE_SUFFIX))
//                    warcFiles.add(status.getPath());
//            }
//        }

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(basePath), true);
        while(files.hasNext()) {
            LocatedFileStatus status = files.next();
            if (status.isFile() && status.getPath().getName().toLowerCase().endsWith(WARC_FILE_EXTENSION))
                warcFiles.add(status.getPath().toString().substring((fs.getUri() + getBasePath()).length()));
        }

        // Return WARC files at this level
        return warcFiles;
    }

    /**
     * Ensures a directory exists at the given path by creating one if nothing exists there. Throws a
     * RunTimeExceptionError if something exists at the path but is not a directory, because there is no way to safely
     * recover from this situation.
     *
     * @param dirPath Path to the directory to create, if it doesn't exist yet.
     */
    @Override
    public void mkdirsIfNeeded(String dirPath) throws IOException {
      Path dir = new Path(getBasePath() + dirPath);


      if (fs.isDirectory(dir)) {
        return;
      }
      if (!fs.mkdirs(dir)) {
        throw new IOException(String.format("Error creating %s: fs.mkdirs() did not succeed", dirPath));
      }
    }

  @Override
  public long getFileLength(String filePath) throws IOException {
    try {
      return fs.getFileStatus(new Path(getBasePath() + filePath)).getLen();
    }
    catch (FileNotFoundException fnfe) {
      return 0L;
    }
  }

  @Override
  public String makeStorageUrl(String filePath, String offset) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", offset);
    return makeStorageUrl(filePath, params);
  }

  @Override
  public String makeStorageUrl(String filePath, MultiValueMap<String, String> params) {
      UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(fs.getUri() + getBasePath() + filePath);
      uriBuilder.queryParams(params);
      return uriBuilder.toUriString();
  }

  @Override
  public void createFileIfNeeded(String filePath) throws IOException {
    Path file = new Path(getBasePath() + filePath);

    log.info(String.format("Creating new HDFS file: %s", file));
    fs.createNewFile(file);
  }

  @Override
  public OutputStream getAppendableOutputStream(String filePath) throws IOException {
    Path extPath = new Path(getBasePath() + filePath);
    log.info(String.format("Opening %s for appendable OutputStream", extPath));
    return fs.append(extPath);
  }

  @Override
  public InputStream getInputStream(String filePath) throws IOException {
    return fs.open(new Path(getBasePath() + filePath));
  }

  @Override
  public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
    FSDataInputStream fsDataInputStream = fs.open(new Path(getBasePath() + filePath));
    fsDataInputStream.seek(seek);
    return fsDataInputStream;
  }

  @Override
  public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
    return getFileAndOffsetWarcRecordInputStream(storageUrl);
  }

  @Override
  public void renameFile(String srcPath, String dstPath) throws IOException {
    if (!fs.rename(new Path(getBasePath() + srcPath), new Path(getBasePath() + dstPath))) {
      throw new IOException(String.format("Error renaming %s to %s", srcPath, dstPath));
    }
  }

  @Override
  public String makeNewStorageUrl(String newPath, Artifact artifact) {
    return makeNewFileAndOffsetStorageUrl(newPath, artifact);
  }

    /**
     * Creates a new WARC file, and begins it with a warcinfo WARC record.
     *
     * @param warcFilePath
     *          A {@code Path} to the new WARC file to create.
     * @throws IOException
     */
    public void createWarcFile(Path warcFilePath) throws IOException {
        if (!fs.exists(warcFilePath)) {
            // Create a new WARC file
            fs.createNewFile(warcFilePath);

            // TODO: Write a warcinfo WARC record

        } else {
            if (!fs.isFile(warcFilePath)) {
                log.warn(String.format("%s is not a file", warcFilePath));
            }
        }
    }

}
