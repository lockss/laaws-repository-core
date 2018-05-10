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
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.lockss.laaws.rs.io.index.*;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Apache Hadoop Distributed File System (HDFS) implementation of WarcArtifactDataStore.
 */
public class HdfsWarcArtifactDataStore extends WarcArtifactDataStore {

  private final static Log log = LogFactory.getLog(HdfsWarcArtifactDataStore.class);

  protected Path base;
  protected FileSystem fs;

  public HdfsWarcArtifactDataStore(Configuration config, String basePath) throws IOException {
    this(config, new Path(basePath));
  }

    /**
     * Constructor.
     *
     * @param config
     *          A Apache Hadoop {@code Configuration}.
     * @param base
     *          A {@code Path} to the base directory of the LOCKSS Repository under HDFS.
     */
    public HdfsWarcArtifactDataStore(Configuration config, Path base) throws IOException {
        this(FileSystem.get(config), base);
    }

    public HdfsWarcArtifactDataStore(FileSystem fs, Path base) throws IOException {
        super(base.toString());
        log.info(String.format("Instantiating a data store under %s", basePath));
        this.base = base;
        this.fs = fs;

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

    public Path getBase() {
      return base;
    }

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param index
     *          An ArtifactIndex to rebuild and populate from WARCs.
     * @throws IOException
     */
    public void rebuildIndex(ArtifactIndex index) throws IOException {
        // Rebuild the index if using volatile index
        if (index.getClass() == VolatileArtifactIndex.class) {
            try {
                rebuildIndex(index, this.base);
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "IOException caught while trying to rebuild index from %s",
                        base
                ));
            }
        }
    }

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param basePath The base path of the local repository.
     * @throws IOException
     */
    public void rebuildIndex(ArtifactIndex index, Path basePath) throws IOException {
        Collection<Path> warcs = scanDirectories(basePath);

        Collection<Path> artifactWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("artifacts" + WARC_FILE_EXTENSION))
                .collect(Collectors.toList());

        // Re-index artifacts first
        for (Path warcFile : artifactWarcFiles) {
            try {
                BufferedInputStream bufferedStream = new BufferedInputStream(fs.open(warcFile));
                for (ArchiveRecord record : WARCReaderFactory.get("HdfsWarcArtifactDataStore", bufferedStream, true)) {
                    log.info(String.format(
                            "Re-indexing artifact from WARC %s record %s from %s",
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                            warcFile
                    ));

                    try {
                        ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

                        if (artifactData != null) {
                            // Set ArtifactData storage URL
                            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("hdfs://" + warcFile);
                            uriBuilder.queryParam("offset", record.getHeader().getOffset());
                            artifactData.setStorageUrl(uriBuilder.toUriString());

                            // Default repository metadata for all ArtifactData objects to be indexed
                            artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
                                    artifactData.getIdentifier(),
                                    false,
                                    false
                            ));

                            // Add artifact to the index
                            index.indexArtifact(artifactData);
                        }
                    } catch (IOException e) {
                        log.error(String.format(
                                "IOException caught while attempting to re-index WARC record %s from %s",
                                record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                                warcFile
                        ));
                    }

                }
            } catch (IOException e) {
                log.error(String.format("IOException caught while attempt to re-index WARC file %s", warcFile));
            }
        }

        // TODO: What follows is loading of artifact repository-specific metadata. It should be generalized to others.

        // Get a collection of repository metadata files
        Collection<Path> repoMetadataWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("lockss-repo" + WARC_FILE_EXTENSION))
                .collect(Collectors.toList());

        // Load repository artifact metadata by "replaying" them
        for (Path metadataFile : repoMetadataWarcFiles) {
            try {
                BufferedInputStream bufferedStream = new BufferedInputStream(fs.open(metadataFile));
                for (ArchiveRecord record : WARCReaderFactory.get("HdfsWarcArtifactDataStore", bufferedStream, true)) {
                    // Parse the JSON into a RepositoryArtifactMetadata object
                    RepositoryArtifactMetadata repoState = new RepositoryArtifactMetadata(
                            IOUtils.toString(record)
                    );

                    String artifactId = repoState.getArtifactId();

                    log.info(String.format(
                            "Replaying repository metadata for artifact %s, from WARC %s record %s in %s",
                            artifactId,
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                            metadataFile
                    ));

                    if (index.artifactExists(artifactId)) {
                        if (repoState.isDeleted()) {
                            log.info(String.format("Removing artifact %s from index", artifactId));
                            index.deleteArtifact(artifactId);
                            continue;
                        }

                        if (repoState.isCommitted()) {
                            log.info(String.format("Marking aritfact %s as committed in index", artifactId));
                            index.commitArtifact(artifactId);
                        }
                    } else {
                        if (!repoState.isDeleted()) {
                            log.warn(String.format("Artifact %s not found in index; skipped replay", artifactId));
                        }
                    }
                }
            } catch (IOException e) {
                log.error(String.format(
                        "IOException caught while attempt to re-index metadata WARC file %s",
                        metadataFile
                ));
            }
        }
    }

    /**
     * Recursively finds artifact WARC files under a given base path.
     *
     * @param basePath The base path to scan recursively for WARC files.
     * @return A collection of paths to WARC files under the given base path.
     * @throws IOException
     */
    public Collection<Path> scanDirectories(Path basePath) throws IOException {
        Collection<Path> warcFiles = new ArrayList<>();

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

//        RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, true);
//        while(files.hasNext()) {
//            LocatedFileStatus status = files.next();
//            if (status.isFile() && status.getPath().getName().toLowerCase().endsWith(WARC_FILE_EXTENSION))
//                warcFiles.add(status.getPath());
//        }

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
      Path dir = new Path(getBasePath(), dirPath);
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
      return fs.getFileStatus(new Path(getBasePath(), filePath)).getLen();
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
      UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("hdfs://" + getBasePath() + filePath);
      uriBuilder.queryParams(params);
      return uriBuilder.toUriString();
  }

  @Override
  public void createFileIfNeeded(String filePath) throws IOException {
    Path file = new Path(getBasePath(), filePath);

    if (!fs.exists(file)) {
        log.info(String.format("Creating new HDFS file: %s", file));
        mkdirsIfNeeded(file.getParent().toString());
        fs.createNewFile(file);
    }
  }

  @Override
  public OutputStream getAppendableOutputStream(String filePath) throws IOException {
    Path extPath = new Path(getBasePath(), filePath);
    log.info(String.format("Opening %s for appendable OutputStream", extPath));
    return fs.append(extPath);
  }

  @Override
  public InputStream getInputStream(String filePath) throws IOException {
    return fs.open(new Path(getBasePath(), filePath));
  }

  @Override
  public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
    FSDataInputStream fsDataInputStream = fs.open(new Path(getBasePath(), filePath));
    fsDataInputStream.seek(seek);
    return fsDataInputStream;
  }

  @Override
  public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
    return getFileAndOffsetWarcRecordInputStream(storageUrl);
  }

  @Override
  public void renameFile(String srcPath, String dstPath) throws IOException {
    if (!fs.rename(new Path(getBasePath(), srcPath), new Path(getBasePath(), dstPath))) {
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
