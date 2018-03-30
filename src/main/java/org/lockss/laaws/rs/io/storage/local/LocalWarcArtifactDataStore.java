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

import org.apache.commons.io.IOUtils;
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
import org.springframework.util.DigestUtils;
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

    private static final String WARC_FILE_SUFFIX = ".warc";
    private static final String AU_ARTIFACTS_WARC = "artifacts" + WARC_FILE_SUFFIX;

    /**
     * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
     *
     * @param repositoryBasePath The base path of the local repository.
     */
    public LocalWarcArtifactDataStore(File repositoryBasePath) {
        log.info(String.format("Loading all WARCs under %s", repositoryBasePath.getAbsolutePath()));

        this.repositoryBasePath = repositoryBasePath;

        // Make sure the base path exists
        mkdirIfNotExist(repositoryBasePath);
    }

    public void rebuildIndex(ArtifactIndex index) {
        rebuildIndex(index, repositoryBasePath);
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
                .filter(file -> file.getName().endsWith("artifacts" + WARC_FILE_SUFFIX))
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
                .filter(file -> file.getName().endsWith("lockss-repo" + WARC_FILE_SUFFIX))
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
                new File(dir, name).isFile() && name.toLowerCase().endsWith(WARC_FILE_SUFFIX)
        )));

        // Return WARC files at this level
        return warcFiles;
    }

    /**
     * Returns the filesystem base path to the archival unit (AU) this artifact belongs in.
     *
     * @param artifactId ArtifactData identifier of an artifact.
     * @return Base path of the AU the artifact belongs in.
     */
    public File getArchicalUnitBasePath(ArtifactIdentifier artifactId) {
        String auidHash = DigestUtils.md5DigestAsHex(artifactId.getAuid().getBytes());
        File auPath = new File(getCollectionBasePath(artifactId) + SEPARATOR + AU_DIR_PREFIX + auidHash);
        mkdirIfNotExist(auPath);
        return auPath;
    }

    /**
     * Returns the filesystem base path to the collection this artifact belongs in.
     *
     * @param artifactId ArtifactData identifier of an artifact.
     * @return Base path of the collection the artifact belongs in.
     */
    public File getCollectionBasePath(ArtifactIdentifier artifactId) {
        File collectionDir = new File(repositoryBasePath + SEPARATOR + artifactId.getCollection());
        mkdirIfNotExist(collectionDir);
        return collectionDir;
    }

    /**
     * Ensures a directory exists at the given path by creating one if nothing exists there. Throws a
     * RunTimeExceptionError if something exists at the path but is not a directory, because there is no way to safely
     * recover from this situation.
     *
     * @param path Path to the directory to create, if it doesn't exist yet.
     */
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
     * Adds an artifact to the repository.
     *
     * @param artifactData An artifact.
     * @return An artifact identifier for artifact reference within this repository.
     * @throws IOException
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

            // Get an OutputStream
            File auBasePath = getArchicalUnitBasePath(artifactId);
            File auArtifactsWarcPath = new File(auBasePath + SEPARATOR + AU_ARTIFACTS_WARC);

            // Set the offset for the record to be appended to the length of the WARC file (i.e., the end)
            long offset = auArtifactsWarcPath.length();

            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + auArtifactsWarcPath.getAbsolutePath());
            uriBuilder.queryParam("offset", offset);
            artifactData.setStorageUrl(uriBuilder.toUriString());

            // Get an appending OutputStream to the WARC file
            FileOutputStream fos = new FileOutputStream(auArtifactsWarcPath, true);

            try {
                // Write artifact to WARC file
                long bytesWritten = this.writeArtifactData(artifactData, fos);

                // Calculate offset of next record
//                offset += bytesWritten;
            } catch (HttpException e) {
                throw new IOException(
                        String.format("Caught HttpException while attempting to write artifact to WARC file: %s", e)
                );
            }

            // Close the file
            fos.flush();
            fos.close();

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
            throw new IllegalArgumentException("Artifact used to reference artifact cannot be null");
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
     * @param metadata   ArtifactData metadata.
     * @throws IOException
     */
    @Override
    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata metadata) throws IOException {

//        if (!isDeleted(artifactId)) {
            // Convert ArtifactMetadata object into a WARC metadata record
            WARCRecordInfo metadataRecord = createWarcMetadataRecord(
//                    getWarcRecordId(indexedData.getWarcFilePath(), indexedData.getWarcRecordOffset()),
                    artifactId.getId(),
                    metadata
            );

            // Get an OutputStream to the AU's metadata file
            String metadataPath = getArchicalUnitBasePath(artifactId) + SEPARATOR + metadata.getMetadataId() + WARC_FILE_SUFFIX;
            FileOutputStream fos = new FileOutputStream(metadataPath, true);

            // Append WARC metadata record to AU's repository metadata file
            writeWarcRecord(metadataRecord, fos);

            // Close the OutputStream
            fos.close();
//        }
        return metadata;
    }

    /**
     * Marks the artifact as committed in the repository by updating the repository metadata for this artifact, and the
     * committed status in the artifact index.
     *
     * @param indexData The artifact identifier of the artifact to commit.
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata commitArtifactData(Artifact indexData)
	throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata repoMetadata = artifact.getRepositoryMetadata();

        // Set the commit flag and write the metadata to disk
        if (!repoMetadata.isDeleted()) {
            repoMetadata.setCommitted(true);
            updateArtifactMetadata(indexData.getIdentifier(), repoMetadata);

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
     * @param indexData The artifact identifier of the artifact to mark as deleted.
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifactData(Artifact indexData)
	throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata repoMetadata = artifact.getRepositoryMetadata();

        if (!repoMetadata.isDeleted()) {
            // Update the repository metadata
            repoMetadata.setCommitted(false);
            repoMetadata.setDeleted(true);

            // Write to disk
            updateArtifactMetadata(indexData.getIdentifier(), repoMetadata);

            // Update the committed flag in the index
//            index.commitArtifact(indexData.getId());
        }

        return repoMetadata;
    }
}