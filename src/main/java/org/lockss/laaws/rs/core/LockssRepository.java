/*
 * Copyright (c) 2017-2020, Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.lang.Ready;
import org.lockss.util.time.Deadline;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeoutException;

/**
 * The LOCKSS Repository API:
 * <p>
 * This is the interface of the abstract LOCKSS repository service.
 */
public interface LockssRepository extends Ready {

  /**
   * Imports artifacts from an archive.
   *
   * @param collectionId A {@link String} containing the collection ID of the artifacts.
   * @param auId         A {@link String} containing the AUID of the artifacts.
   * @param inputStream  The {@link InputStream} of the archive.
   * @param isCompressed A {@code boolean} indicating whether the archive is GZIP compressed.
   * @return
   */
  // FIXME: This may need an enum or MIME type specifying the type of archive
  Iterable<ImportStatus> addArtifacts(String collectionId, String auId, InputStream inputStream,
                                      boolean isCompressed) throws IOException;

  /**
   * NEVER: Artifact content should never be included. The client does not want it, period.
   * IF_SMALL: Include the artifact content if the artifact is small enough.
   * ALWAYS: Artifact content must be included.
   */
  enum IncludeContent {
    NEVER,
    IF_SMALL,
    ALWAYS
  }

  default void initRepository() throws IOException {
    // NOP
  }

  default void shutdownRepository() throws InterruptedException {
    // NOP
  }

  /**
   * Adds an artifact to this LOCKSS repository.
   *
   * @param artifactData {@code ArtifactData} instance to add to this LOCKSS repository.
   * @return The artifact ID of the newly added artifact.
   * @throws IOException
   */
  Artifact addArtifact(ArtifactData artifactData) throws IOException;

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param artifact An artifact to retrieve from this repository.
   * @return The {@code ArtifactData} referenced by this artifact.
   * @throws IOException
   */
  default ArtifactData getArtifactData(Artifact artifact) throws IOException {
    return getArtifactData(artifact.getCollection(), artifact.getId());
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param artifact           An artifact to retrieve from this repository.
   * @param includeContent A {@link IncludeContent} indicating whether the artifact content should be included in the
   *                       {@link ArtifactData} returned by this method.
   * @return The {@code ArtifactData} referenced by this artifact.
   * @throws IOException
   */
  default ArtifactData getArtifactData(Artifact artifact,
                                       IncludeContent includeContent)
      throws IOException {
    return getArtifactData(artifact.getCollection(), artifact.getId(),
        includeContent);
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param collection         The collection ID of the artifact.
   * @param artifactId         A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @param includeContent A {@link IncludeContent} indicating whether the artifact content should be included in the
   *                       {@link ArtifactData} returned by this method.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  default ArtifactData getArtifactData(String collection,
                                       String artifactId,
                                       IncludeContent includeContent)
      throws IOException {
    return getArtifactData(collection, artifactId);
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param collection The collection ID of the artifact.
   * @param artifactId A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  ArtifactData getArtifactData(String collection,
                               String artifactId)
      throws IOException;

  /**
   * Returns the headers of an artifact.
   *
   * @param collection The collection ID of the artifact.
   * @param artifactId A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return A {@link HttpHeaders} containing the artifact's headers.
   * @throws IOException
   */
  // Q: Use non-Spring HttpHeaders?
  HttpHeaders getArtifactHeaders(String collection, String artifactId) throws IOException;

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param artifact A {code String} containing the collection ID of the collection containing the artifact to commit.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  default Artifact commitArtifact(Artifact artifact) throws IOException {
    return commitArtifact(artifact.getCollection(), artifact.getId());
  }

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param collection A {code String} containing the collection ID of the collection containing the artifact to commit.
   * @param artifactId A {@code String} with the artifact ID of the artifact to commit to the repository.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  Artifact commitArtifact(String collection, String artifactId) throws IOException;

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param artifact The artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  default void deleteArtifact(Artifact artifact) throws IOException {
    deleteArtifact(artifact.getCollection(), artifact.getId());
  }

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param collection A {code String} containing the collection ID containing the artifact to delete.
   * @param artifactId A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  void deleteArtifact(String collection,
                      String artifactId)
      throws IOException;

  /**
   * Checks whether an artifact is committed to this LOCKSS repository.
   *
   * @param artifactId A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether the artifact is committed.
   */
  Boolean isArtifactCommitted(String collection, String artifactId) throws IOException;

  /**
   * Provides the collection identifiers of the committed artifacts in the index.
   *
   * @return An {@code Iterable<String>} with the index committed artifacts
   * collection identifiers.
   */
  Iterable<String> getCollectionIds() throws IOException;

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
   *
   * @param collection A {@code String} containing the LOCKSS repository collection ID.
   * @return A {@code Iterable<String>} iterating over the AUIDs in this LOCKSS repository collection.
   * @throws IOException
   */
  Iterable<String> getAuIds(String collection) throws IOException;

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterable<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException
   */
  Iterable<Artifact> getArtifacts(String collection,
                                  String auid)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
   */
  Iterable<Artifact> getArtifactsAllVersions(String collection,
                                             String auid)
      throws IOException;

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterable<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException
   */
  Iterable<Artifact> getArtifactsWithPrefix(String collection,
                                            String auid,
                                            String prefix)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection,
                                                       String auid,
                                                       String prefix)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified collection.
   *
   * @param collection A String with the collection identifier.
   * @param prefix     A String with the URL prefix.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String collection,
                                                         String prefix,
                                                         ArtifactVersions versions)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  Iterable<Artifact> getArtifactsAllVersions(String collection,
                                             String auid,
                                             String url)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  Iterable<Artifact> getArtifactsWithUrlFromAllAus(String collection,
                                                   String url,
                                                   ArtifactVersions versions)
      throws IOException;

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param url        A {@code String} containing a URL.
   * @return The {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException
   */
  Artifact getArtifact(String collection,
                       String auid,
                       String url)
      throws IOException;

  /**
   * Returns the artifact with the specified artifactId
   *
   * @param artifactId
   * @return The {@code Artifact} representing that artifactId, or null
   * if none
   * @throws IOException
   */
  Artifact getArtifactFromId(String artifactId)
      throws IOException;

  /**
   * Returns the committed artifact of a given version of a URL, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
   */
  default Artifact getArtifactVersion(String collection,
                                      String auid,
                                      String url,
                                      Integer version)
      throws IOException {
    return getArtifactVersion(collection, auid, url, version, false);
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
   *
   * @param collection         A String with the collection identifier.
   * @param auid               A String with the Archival Unit identifier.
   * @param url                A String with the URL to be matched.
   * @param version            A String with the version.
   * @param includeUncommitted A boolean with the indication of whether an uncommitted artifact
   *                           may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
   */
  Artifact getArtifactVersion(String collection,
                              String auid,
                              String url,
                              Integer version,
                              boolean includeUncommitted)
      throws IOException;


  /**
   * Returns the size, in bytes, of AU in a collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return A {@link AuSize} with byte size statistics of the specified AU.
   */
  AuSize auSize(String collection, String auid) throws IOException;

  /**
   * Returns information about the repository's storage areas
   *
   * @return A {@code RepositoryInfo}
   * @throws IOException
   */
  RepositoryInfo getRepositoryInfo() throws IOException;

  long DEFAULT_WAITREADY = 5000;

  @Override
  default void waitReady(Deadline deadline) throws TimeoutException {
    final L4JLogger log = L4JLogger.getLogger();

    while (!isReady()) {
      if (deadline.expired()) {
        throw new TimeoutException("Deadline for repository to become ready expired");
      }

      long remainingTime = deadline.getRemainingTime();
      long sleepTime = Math.min(deadline.getSleepTime(), DEFAULT_WAITREADY);

      log.debug(
          "Waiting for repository to become ready; retrying in {} ms (deadline in {} ms)",
          sleepTime,
          remainingTime
      );

      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for repository to become ready");
      }
    }
  }
}
