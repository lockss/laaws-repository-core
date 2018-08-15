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

package org.lockss.laaws.rs.io.storage.warc;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.io.warc.WARCRecord;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URLEncoder;
import java.util.*;

/**
 * A volatile ("in-memory") implementation of WarcArtifactDataStore.
 */
public class VolatileWarcArtifactDataStore extends WarcArtifactDataStore {
    private final static Log log = LogFactory.getLog(VolatileWarcArtifactDataStore.class);
    private Map<String, Map<String, Map<String, byte[]>>> repository;
    private Map<String, RepositoryArtifactMetadata> repositoryMetadata;

    /**
     * Constructor.
     */
    public VolatileWarcArtifactDataStore() {
      this("volatile:///");
    }

    /**
     * For testing; this kind of data store ignores the base path.
     */
    protected VolatileWarcArtifactDataStore(String basePath) {
        super(basePath);
        this.repository = new HashMap<>();
        this.repositoryMetadata = new HashMap<>();
    }    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
        if (artifactData == null) {
          throw new NullPointerException("artifactData is null");
        }

        // Get artifact identifier
        ArtifactIdentifier ident = artifactData.getIdentifier();

        // Get the collection
        Map<String, Map<String, byte[]>> collection = getInitialize(repository, ident.getCollection(), new HashMap<>());

        // Get the AU
        Map<String, byte[]> au = getInitialize(collection, ident.getAuid(), new HashMap<>());

        try {
            // ByteArrayOutputStream to capture the WARC record
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // Write artifact as a WARC record stream to the OutputStream
            long bytesWritten = writeArtifactData(artifactData, baos);

            // Store artifact
            au.put(ident.getId(), baos.toByteArray());
        } catch (HttpException e) {
            log.error(String.format("Caught an HttpException while attempt to write an ArtifactData to an OutputStream: %s", e.getMessage()));
            throw new IOException(e);
        }

        // Create and set the artifact's repository metadata
        RepositoryArtifactMetadata repoMetadata = new RepositoryArtifactMetadata(ident, false, false);
        repositoryMetadata.put(ident.getId(), repoMetadata);
        artifactData.setRepositoryMetadata(repoMetadata);

        // Construct volatile storage URL for this WARC record
        String storageUrl = makeStorageUrl(ident);

        // Set the artifact's storage URL
        artifactData.setStorageUrl(storageUrl);

        // Create an Artifact to return
        Artifact artifact = new Artifact(
                ident,
                false,
                storageUrl,
                artifactData.getContentLength(),
                artifactData.getContentDigest()
        );

        return artifact;
    }//  @Test
//  public void testMkdirsIfNeeded() throws Exception {
//  File tmp1 = makeTempDir();
//  String dirPath = tmp1.getAbsolutePath() + "/foo/bar/baz";
//  File dir = new File(dirPath);
//  assertFalse(dir.exists());
//  LocalWarcArtifactDataStore.mkdirsIfNeeded(dirPath);
//  assertTrue(dir.isDirectory());
//  LocalWarcArtifactDataStore.mkdirsIfNeeded(dirPath); // should not fail or throw
//  quietlyDeleteDir(tmp1);
//}


    @Override
    public ArtifactData getArtifactData(Artifact artifact) throws IOException {
        // Cannot work with a null Artifact
        if (artifact == null) {
            throw new NullPointerException("artifact is null");
        }
        // ArtifactData to return; defaults to null if one could not be found
        ArtifactData artifactData = null;

        // Get the map representing an artifact collection
        if (repository.containsKey(artifact.getCollection())) {
            // Get the collection of artifacts
            Map<String, Map<String, byte[]>> collection = repository.get(artifact.getCollection());

            // Get the map representing an AU from collection
            if (collection.containsKey(artifact.getAuid())) {
                Map<String, byte[]> au = collection.getOrDefault(artifact.getAuid(), new HashMap<>());

                // Retrieve the artifact's byte stream (artifact is encoded as a WARC record stream here)
                byte[] artifactBytes = au.get(artifact.getId());

                // Adapt byte array to ArtifactData
                if (artifactBytes != null) {
                    InputStream warcRecordStream = new ByteArrayInputStream(artifactBytes);

                    // Assemble a WARCRecord object using the WARC record bytestream in memory
                    WARCRecord record = new WARCRecord(
                            warcRecordStream,
                            null,
                            0,
                            true,
                            true
                    );

                    // Generate an artifact from the HTTP response stream
                    artifactData = ArtifactDataFactory.fromHttpResponseStream(record);

                    // Save the underlying input stream so that it can be closed when needed.
                    artifactData.setClosableInputStream(warcRecordStream);

                    // Set ArtifactData properties
                    artifactData.setIdentifier(artifact.getIdentifier());
                    artifactData.setStorageUrl(artifact.getStorageUrl());
                    artifactData.setContentLength(artifact.getContentLength());
                    artifactData.setContentDigest(artifact.getContentDigest());
                    artifactData.setRepositoryMetadata(repositoryMetadata.get(artifact.getId()));
                }
            }
        }

        return artifactData;
    }

    /**
     * Updates and writes associated metadata of an artifact to this store.
     *
     * @param artifactId
     *          A (@code ArtifactIdentifier) that identifies the artifact to update.
     * @param artifactMetadata
     *          RepositoryArtifactMetadata update the artifact with, and write to the store.
     * @return A representation of the RepositoryArtifactMetadata as it is now stored.
     */
    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) {
        if (artifactId == null) {
          throw new NullPointerException("artifactId is null");
        }
        if (artifactMetadata == null) {
          throw new NullPointerException("artifactMetadata is null");
        }
        repositoryMetadata.replace(artifactId.getId(), artifactMetadata);
        return repositoryMetadata.get(artifactId.getId());
    }

    /**
     * Commits an artifact to this artifact store.
     *
     * @param artifact
     *          A (@code ArtifactIdentifier) that identifies the artifact to commit and store permanently.
     * @return A {@code RepositoryArtifactMetadata} updated to indicate the new commit status as it is now stored.
     */
    @Override
    public RepositoryArtifactMetadata commitArtifactData(Artifact artifact) {
        if (artifact == null) {
          throw new NullPointerException("artifact is null");
        }

        RepositoryArtifactMetadata metadata = repositoryMetadata.get(artifact.getId());
        metadata.setCommitted(true);

        // TODO: Use updateArtifactMetadata
        repositoryMetadata.replace(artifact.getId(), metadata);
        return metadata;
    }

    /**
     * Removes an artifact from this store.
     *
     * @param artifact
     *          A {@code Artifact} referring to the artifact to remove from this store.
     * @return A {@code RepositoryArtifactMetadata} updated to indicate the deleted status of this artifact.
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifactData(Artifact artifact) {
        if (artifact == null) {
            throw new NullPointerException("artifact is null");
        }
        
        Map<String, Map<String, byte[]>> collection = repository.get(artifact.getCollection());
        Map<String, byte[]> au = collection.get(artifact.getAuid());
        au.remove(artifact.getId());

        // TODO: Use updateArtifactMetadata
        RepositoryArtifactMetadata metadata = repositoryMetadata.get(artifact.getId());
        metadata.setDeleted(true);
        repositoryMetadata.replace(artifact.getId(), metadata);

        repositoryMetadata.remove(artifact.getId());
        return metadata;
    }

  /**
   * <p>
   * Given a map, either returns the value associated with the given key (if the
   * key is associated with a non-null value), or sets a mapping from the given
   * initial value and returns it (if the key is not associated with a value or
   * is associated with {@code null}).
   * </p>
   * <p>
   * This is slightly different from {@link Map#putIfAbsent(Object, Object)},
   * which returns {@code null} if the key is not associated with a value or is
   * associated with {@code null}, which does not enable the caller to use the
   * returned value immediately.
   * </p>
   * 
   * @param map
   *          A map.
   * @param key
   *          A key in the map.
   * @param initial
   *          An initial value used to create a mapping with the key if the key
   *          is not associated with a value or is associated with {@code null}
   *          in the map.
   * @param <K>
   *          The key type in the map.
   * @param <T>
   *          The value type in the map.
   * @return The value associated with the key after the side effect: either the
   *         original value if the key was associated with one, or the initial
   *         value used to initialize a mapping with the key if they key was not
   *         associated with a value or was associated with {@code null}.
   */
    protected static <K, T> T getInitialize(Map<K, T> map, K key, T initial) {
      T current = map.putIfAbsent(key, initial);
      return current == null ? initial : current;
    }
    
  @Override
  public void mkdirsIfNeeded(String dirPath) throws IOException {
    // Intentionally left blank
  }
  
  @Override
  public long getFileLength(String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public String makeStorageUrl(ArtifactIdentifier ident) {
    try {
      return makeStorageUrl(String.format("/%s/%s/%s/%s/%s",
                                          ident.getCollection(),
                                          ident.getAuid(),
                                          URLEncoder.encode(ident.getUri(), "UTF-8"),
                                          ident.getVersion(),
                                          ident.getId()),
                            0L);
    }
    catch (UnsupportedEncodingException shouldnt) {
      log.error("Internal error", shouldnt);
      throw new UncheckedIOException(shouldnt);
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
      UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("volatile://" + getBasePath() + filePath);
      uriBuilder.queryParams(params);
      return uriBuilder.toUriString();
    }

    @Override
  public OutputStream getAppendableOutputStream(String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getInputStream(String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }  

  @Override
  public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void createFileIfNeeded(String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void renameFile(String srcPath, String dstPath) throws IOException {
    throw new UnsupportedOperationException();
  }

    @Override
    public Collection<String> scanDirectories(String basePath) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
  public String makeNewStorageUrl(String newPath, Artifact artifact) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * Always true in volatile implementation.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return true;
  }
}
