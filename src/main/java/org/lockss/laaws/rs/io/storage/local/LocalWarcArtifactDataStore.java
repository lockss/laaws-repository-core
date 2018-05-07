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

package org.lockss.laaws.rs.io.storage.local;

import org.apache.commons.io.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Local filesystem implementation of WarcArtifactDataStore.
 */
public class LocalWarcArtifactDataStore extends WarcArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata> {
    private static final Log log = LogFactory.getLog(LocalWarcArtifactDataStore.class);

    private static final String AU_ARTIFACTS_WARC_NAME = "artifacts" + WARC_FILE_EXTENSION;

    private static final long THRESHOLD_WARC_SIZE = 100L * FileUtils.ONE_MB;

    protected File repositoryBase;

    public LocalWarcArtifactDataStore(String repositoryBasePath) throws IOException {
      super(repositoryBasePath);
      log.info(String.format("Loading all WARCs under %s", repositoryBasePath));
      mkdirsIfNeeded(repositoryBasePath);
      this.repositoryBase = new File(repositoryBasePath);
      
      // Initialize sealed WARC directory
      try {
        mkdirsIfNeeded(getSealedWarcPath());
      }
      catch (IOException ioe) {
        throw new IOException("Could not create sealed WARC directory " + getSealedWarcPath(), ioe);
      }
    }
    
    /**
     * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
     *
     * @param repositoryBasePath The base path of the local repository.
     * @deprecated Use {@link #LocalWarcArtifactDataStore(String)}
     */
    @Deprecated
    public LocalWarcArtifactDataStore(File repositoryBase) throws IOException {
      this(repositoryBase.getAbsolutePath());
    }

    public void rebuildIndex(ArtifactIndex index) {
        rebuildIndex(index, repositoryBase);
    }

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param basePath The base path of the local repository.
     */
    public void rebuildIndex(ArtifactIndex index, File basePath) {
        Collection<File> warcs = scanDirectories(basePath);

        Collection<File> artifactWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("artifacts" + WARC_FILE_EXTENSION))
                .collect(Collectors.toList());

        // Re-index artifacts first
        for (File warcFile : artifactWarcFiles) {
            log.info(String.format("Reading artifacts from %s", warcFile));

            try {
                for (ArchiveRecord record : WARCReaderFactory.get(warcFile)) {
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
                            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + warcFile.getAbsolutePath());
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
        Collection<File> repoMetadataWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("lockss-repo" + WARC_FILE_EXTENSION))
                .collect(Collectors.toList());

        // Update LOCKSS repository metadata for artifacts by "replaying" their changes
        for (File metadataFile : repoMetadataWarcFiles) {
            log.info(String.format("Reading repository metadata journal from %s", metadataFile));

            try {
                for (ArchiveRecord record : WARCReaderFactory.get(metadataFile)) {
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
     */
    public static Collection<File> scanDirectories(File basePath) {
        Collection<File> warcFiles = new ArrayList<>();

        // DFS recursion through directories
        Arrays.stream(basePath.listFiles(x -> x.isDirectory()))
                .map(x -> scanDirectories(x))
                .forEach(warcFiles::addAll);

        // Add WARC files from this directory
        warcFiles.addAll(Arrays.asList(basePath.listFiles((dir, name) ->
                new File(dir, name).isFile() && name.toLowerCase().endsWith(WARC_FILE_EXTENSION)
        )));

        // Return WARC files at this level
        return warcFiles;
    }

    public String getAuArtifactsWarcPath(ArtifactIdentifier artifactId) throws IOException {
      String dir = getAuPath(artifactId);
      mkdirsIfNeeded(dir);
      return dir + SEPARATOR + AU_ARTIFACTS_WARC_NAME;
    }

    public String getAuMetadataWarcPath(ArtifactIdentifier artifactId,
                                        RepositoryArtifactMetadata artifactMetadata)
        throws IOException {
      String dir = getAuPath(artifactId);
      mkdirsIfNeeded(dir);
      return dir + SEPARATOR + artifactMetadata.getMetadataId() + WARC_FILE_EXTENSION;
    }

    /**
     * Ensures a directory exists at the given path by creating one if nothing exists there. Throws a
     * RunTimeExceptionError if something exists at the path but is not a directory, because there is no way to safely
     * recover from this situation.
     *
     * @param path Path to the directory to create, if it doesn't exist yet.
     * @deprecated
     */
    @Deprecated
    public static void mkdirIfNotExist(File path) {
        if (path.exists()) {
            // YES: Make sure it is a directory
            if (!path.isDirectory()) {
                throw new RuntimeException(String.format("%s exists but is not a directory", path));
            }
        } else {
            // NO: Create a directory for the collection
            path.mkdir();
        }
    }

    /**
     * Ensures a directory exists at the given path by creating one if nothing exists there. Throws a
     * RunTimeExceptionError if something exists at the path but is not a directory, because there is no way to safely
     * recover from this situation.
     *
     * @param path Path to the directory to create, if it doesn't exist yet.
     */
    public static void mkdirsIfNeeded(String dirPath) throws IOException {
      File dir = new File(dirPath);
      if (dir.exists()) {
        if (!dir.isDirectory()) {
          throw new IOException(String.format("Error creating %s: exists but is not a directory", dir.getAbsolutePath()));
        }
      }
      else {
        if (!dir.mkdirs()) {
          throw new IOException(String.format("Error creating %s: mkdirs did not succeed", dir.getAbsolutePath()));
        }
      }
    }

    protected String makeStorageUrl(String filePath, String offset) {
      UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + filePath);
      uriBuilder.queryParam("offset", offset);
      return uriBuilder.toUriString();
    }
    
    protected String makeStorageUrl(String filePath, long offset) {
      return makeStorageUrl(filePath, Long.toString(offset));
    }

    protected String updateStorageUrl(String filePath, Artifact artifact) {
      String oldUrl = artifact.getStorageUrl();
      return makeStorageUrl(filePath, oldUrl.substring(oldUrl.lastIndexOf("?") + 1));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
        if (artifactData == null) {
          throw new NullPointerException("artifactData is null");
        }
          
//        if (index == null) {
//             YES: Cannot proceed without an artifact index - throw RuntimeException
//            throw new RuntimeException("No artifact index configured!");
//
//        } else {
            // NO: Add the ArtifactData to the index
            ArtifactIdentifier artifactId = artifactData.getIdentifier();

            // Set new artifactId - any existing artifactId is meaningless in this context and should be discarded
            artifactId.setId(UUID.randomUUID().toString());

            log.info(String.format(
                    "Adding artifact (%s, %s, %s, %s, %s)",
                    artifactId.getId(),
                    artifactId.getCollection(),
                    artifactId.getAuid(),
                    artifactId.getUri(),
                    artifactId.getVersion()
            ));

            String auArtifactsWarcPath = getAuArtifactsWarcPath(artifactId);
            File auArtifactsWarc = new File(auArtifactsWarcPath);

            // Set the offset for the record to be appended to the length of the WARC file (i.e., the end)
            long offset = auArtifactsWarc.length();
            long bytesWritten = -1L;

            artifactData.setStorageUrl(makeStorageUrl(auArtifactsWarcPath, offset));

            try (
                // Get an appending OutputStream to the WARC file
                FileOutputStream fos = new FileOutputStream(auArtifactsWarc, true)
            ) {
                // Write artifact to WARC file
                bytesWritten = writeArtifactData(artifactData, fos);

                // Calculate offset of next record
                offset += bytesWritten;
                
                fos.flush();
            } catch (HttpException e) {
                throw new IOException(
                        String.format("Caught HttpException while attempting to write artifact to WARC file: %s", e)
                );
            }

            if (offset >= THRESHOLD_WARC_SIZE) {
                String newPath = sealWarc(artifactId.getCollection(),
                                          artifactId.getAuid(),
                                          auArtifactsWarcPath,
                                          this::updateStorageUrl);
                artifactData.setStorageUrl(makeStorageUrl(newPath, offset - bytesWritten));
            }
            
            // Attach the artifact's repository metadata
            artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
                    artifactId,
                    false,
                    false
            ));

            // TODO: Generalize this to write all of an artifact's metadata
            updateArtifactMetadata(artifactId, artifactData.getRepositoryMetadata());

        Artifact artifact = new Artifact(
                artifactId,
                false,
                artifactData.getStorageUrl(),
                artifactData.getContentLength(),
                artifactData.getContentDigest()
        );

        return artifact;
    }

    @Override
    public ArtifactData getArtifactData(Artifact artifact) throws IOException {
        if (artifact == null) {
          throw new NullPointerException("artifact is null");
        }
        log.info(String.format("Retrieving artifact from store (artifactId: %s)", artifact.getId()));

        // Get InputStream to WARC file
        URI uri = urlToUri(artifact.getStorageUrl());
//        String warcFilePath = uri.getScheme() + uri.getAuthority() + uri.getPath();
        String warcFilePath = uri.getPath();
        InputStream warcStream = new FileInputStream(warcFilePath);

        // Seek to the WARC record
        List<String> offsetQueryArgs = UriComponentsBuilder.fromUri(uri).build().getQueryParams().get("offset");

        if (offsetQueryArgs != null && !offsetQueryArgs.isEmpty()) {
          warcStream.skip(Long.parseLong(offsetQueryArgs.get(0)));
        }

        // Get a WARCRecord object
        WARCRecord record = new WARCRecord(warcStream, "LocalWarcArtifactDataStore#getArtifact", 0);

        // Convert the WARCRecord object to an ArtifactData
        ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

        // Repository metadata for this artifact
        RepositoryArtifactMetadata repoMetadata = new RepositoryArtifactMetadata(
                artifact.getIdentifier(),
                artifact.getCommitted(),
                false
        );

        // Set ArtifactData properties
        artifactData.setIdentifier(artifact.getIdentifier());
        artifactData.setStorageUrl(artifact.getStorageUrl());
        artifactData.setContentLength(artifact.getContentLength());
        artifactData.setContentDigest(artifact.getContentDigest());
        artifactData.setRepositoryMetadata(repoMetadata);

        // Return an ArtifactData from the WARC record
        return artifactData;
    }

    /**
     * Updates the metadata of an artifact by appending a WARC metadata record to a metadata WARC file.
     *
     * @param artifactId The artifact identifier to add the metadata to.
     * @param artifactMetadata   ArtifactData metadata.
     * @throws IOException
     */
    @Override
    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) throws IOException {
        if (artifactId == null) {
          throw new NullPointerException("artifactId is null");
        }
        if (artifactMetadata == null) {
          throw new NullPointerException("artifactMetadata is null");
        }

//        if (!isDeleted(artifactId)) {
            // Convert ArtifactMetadata object into a WARC metadata record
            WARCRecordInfo metadataRecord = createWarcMetadataRecord(
//                    getWarcRecordId(indexedData.getWarcFilePath(), indexedData.getWarcRecordOffset()),
                    artifactId.getId(),
                    artifactMetadata
            );

            // Append WARC metadata record to AU's metadata file
            String metadataPath = getAuMetadataWarcPath(artifactId, artifactMetadata);
            try (
              FileOutputStream fos = new FileOutputStream(metadataPath, true)
            ) {
              writeWarcRecord(metadataRecord, fos);
            }

//        }
        return artifactMetadata;
    }

    /**
     * Marks the artifact as committed in the repository by updating the repository metadata for this artifact, and the
     * committed status in the artifact index.
     *
     * @param artifact The artifact identifier of the artifact to commit.
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata commitArtifactData(Artifact artifact)
	throws IOException {
        if (artifact == null) {
          throw new NullPointerException("artifact is null");
        }
        ArtifactData artifactData = getArtifactData(artifact);
        RepositoryArtifactMetadata repoMetadata = artifactData.getRepositoryMetadata();

        // Set the commit flag and write the metadata to disk
        if (!repoMetadata.isDeleted()) {
            repoMetadata.setCommitted(true);
            updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);

            // Update the committed flag in the index
//            index.commitArtifact(indexData.getId());
        }

        return repoMetadata;
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as deleted in the repository storage.
     *
     * @param indexData The artifact identifier of the aritfact to check.
     * @return A boolean indicating whether the artifact is marked as deleted.
     * @throws IOException
     * @throws URISyntaxException 
     */
    public boolean isDeleted(Artifact indexData)
	throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isDeleted();
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as committed in the repository storage.
     *
     * @param indexData The artifact identifier of the artifact to check.
     * @return A boolean indicating whether the artifact is marked as committed.
     * @throws IOException
     * @throws URISyntaxException 
     */
    public boolean isCommitted(Artifact indexData)
	throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isCommitted();
    }

    /**
     * Marks the artifact as deleted in the repository by updating the repository metadata for this artifact.
     *
     * @param artifact The artifact identifier of the artifact to mark as deleted.
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifactData(Artifact artifact)
	throws IOException {
        if (artifact == null) {
          throw new NullPointerException("artifact is null");
        }
        ArtifactData artifactData = getArtifactData(artifact);
        RepositoryArtifactMetadata repoMetadata = artifactData.getRepositoryMetadata();

        if (!repoMetadata.isDeleted()) {
            // Update the repository metadata
            repoMetadata.setCommitted(false);
            repoMetadata.setDeleted(true);

            // Write to disk
            updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);

            // Update the committed flag in the index
//            index.commitArtifact(indexData.getId());
        }

        return repoMetadata;
    }

}