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

  enum ArchiveType {
    ARC,
    WARC
  }

  /**
   * Imports artifacts from an archive.
   *
   * @param namespace A {@link String} containing the namespace of the artifacts.
   * @param auId         A {@link String} containing the AUID of the artifacts.
   * @param inputStream  The {@link InputStream} of the archive.
   * @param type         A {@link ArchiveType} indicating the type of archive.
   * @param isCompressed A {@code boolean} indicating whether the archive is GZIP compressed.
   * @return
   */
  ImportStatusIterable addArtifacts(String namespace, String auId, InputStream inputStream,
                                      ArchiveType type, boolean isCompressed) throws IOException;

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
    return getArtifactData(artifact.getNamespace(), artifact.getUuid());
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
    return getArtifactData(artifact.getNamespace(), artifact.getUuid(),
        includeContent);
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param namespace         The namespace of the artifact.
   * @param artifactUuid         A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @param includeContent A {@link IncludeContent} indicating whether the artifact content should be included in the
   *                       {@link ArtifactData} returned by this method.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  default ArtifactData getArtifactData(String namespace,
                                       String artifactUuid,
                                       IncludeContent includeContent)
      throws IOException {
    return getArtifactData(namespace, artifactUuid);
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   * <br>(See Reusability and release note in {@link
   * org.lockss.laaws.rs.model.ArtifactData})
   *
   * @param namespace The namespace of the artifact.
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  ArtifactData getArtifactData(String namespace,
                               String artifactUuid)
      throws IOException;

  /**
   * Returns the headers of an artifact.
   *
   * @param namespace The namespace of the artifact.
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return A {@link HttpHeaders} containing the artifact's headers.
   * @throws IOException
   */
  // Q: Use non-Spring HttpHeaders?
  HttpHeaders getArtifactHeaders(String namespace, String artifactUuid) throws IOException;

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param artifact An {@link Artifact} to commit.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  default Artifact commitArtifact(Artifact artifact) throws IOException {
    return commitArtifact(artifact.getNamespace(), artifact.getUuid());
  }

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param namespace A {code String} containing the namespace of the artifact to commit.
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to commit to the repository.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  Artifact commitArtifact(String namespace, String artifactUuid) throws IOException;

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param artifact The artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  default void deleteArtifact(Artifact artifact) throws IOException {
    deleteArtifact(artifact.getNamespace(), artifact.getUuid());
  }

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param namespace A {code String} containing the namespace of the artifact to delete.
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  void deleteArtifact(String namespace,
                      String artifactUuid)
      throws IOException;

  /**
   * Checks whether an artifact is committed to this LOCKSS repository.
   *
   * @param namespace A {code String} containing the namespace of the artifact.
   * @param artifactUuid A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether the artifact is committed.
   */
  Boolean isArtifactCommitted(String namespace, String artifactUuid) throws IOException;

  /**
   * Returns the namespace of the committed artifacts in the index.
   *
   * @return An {@code Iterable<String>} with the index committed artifacts namespaces.
   */
  Iterable<String> getNamespaces() throws IOException;

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in a namespace.
   *
   * @param namespace A {code String} containing the namespace of the artifact.
   * @return A {@code Iterable<String>} iterating over the AUIDs in this namespace.
   * @throws IOException
   */
  Iterable<String> getAuIds(String namespace) throws IOException;

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace A {code String} containing the namespace of the artifact.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterable<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException
   */
  Iterable<Artifact> getArtifacts(String namespace, String auid) throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
   */
  Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                             String auid)
      throws IOException;

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterable<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException
   */
  Iterable<Artifact> getArtifactsWithPrefix(String namespace,
                                            String auid,
                                            String prefix)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace,
                                                       String auid,
                                                       String prefix)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified namespace.
   *
   * @param namespace A String with the namespace.
   * @param prefix     A String with the URL prefix.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace,
                                                         String prefix,
                                                         ArtifactVersions versions)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                             String auid,
                                             String url)
      throws IOException;

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param url        A {@code String} with the URL to be matched.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace,
                                                   String url,
                                                   ArtifactVersions versions)
      throws IOException;

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param url        A {@code String} containing a URL.
   * @return The {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException
   */
  Artifact getArtifact(String namespace,
                       String auid,
                       String url)
      throws IOException;

  /**
   * Returns the artifact with the specified artifact UUID.
   *
   * @param artifactUuid
   * @return The {@code Artifact} representing that artifact UUID, or null
   * if none
   * @throws IOException
   */
  Artifact getArtifactFromUuid(String artifactUuid)
      throws IOException;

  /**
   * Returns the committed artifact of a given version of a URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
   */
  default Artifact getArtifactVersion(String namespace,
                                      String auid,
                                      String url,
                                      Integer version)
      throws IOException {
    return getArtifactVersion(namespace, auid, url, version, false);
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and namespace.
   *
   * @param namespace         A String with the namespace.
   * @param auid               A String with the Archival Unit identifier.
   * @param url                A String with the URL to be matched.
   * @param version            A String with the version.
   * @param includeUncommitted A boolean with the indication of whether an uncommitted artifact
   *                           may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
   */
  Artifact getArtifactVersion(String namespace,
                              String auid,
                              String url,
                              Integer version,
                              boolean includeUncommitted)
      throws IOException;


  /**
   * Returns the size, in bytes, of AU in a namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return A {@link AuSize} with byte size statistics of the specified AU.
   */
  AuSize auSize(String namespace, String auid) throws IOException;

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
