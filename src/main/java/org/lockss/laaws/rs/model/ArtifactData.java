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

package org.lockss.laaws.rs.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.http.StatusLine;
import org.lockss.log.L4JLogger;
import org.lockss.util.CloseCallbackInputStream;
import org.lockss.util.io.EofRememberingInputStream;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * An {@code ArtifactData} serves as an atomic unit of data archived in the
 * LOCKSS Repository.
 * <br>
 * Reusability and release:<ul>
 * <li>{@link #getInputStream()} may be called only once.</li>
 * <li>Once an ArtifactData is obtained, it <b>must</b> be released (by
 * calling {@link #release()}, whether or not {@link #getInputStream()} has
 * been called.
 * </ul>
 */
public class ArtifactData implements Comparable<ArtifactData>, AutoCloseable {
  private final static L4JLogger log = L4JLogger.getLogger();
  public static final String DEFAULT_DIGEST_ALGORITHM = "SHA-256";

  // Core artifact attributes
  private ArtifactIdentifier identifier;

  // Artifact data stream
  private InputStream artifactStream;
  private CountingInputStream cis;
  private DigestInputStream dis;
  private EofRememberingInputStream eofis;
  private boolean isComputeDigestOnRead = false;

  // The byte stream of this artifact before it is wrapped in the WARC
  // processing code that does not honor close().
  private InputStream closableInputStream;

  // Artifact data properties
  private HttpHeaders artifactMetadata; // TODO: Switch from Spring to Apache?
  private StatusLine httpStatus;
  private InputStream origInputStream;
  private boolean hadAnInputStream = false;
  private long contentLength = -1;
  private String contentDigest;

  // Internal repository state
  private ArtifactRepositoryState artifactRepositoryState;
  private URI storageUrl;

  // The collection date.
  private long collectionDate = -1;
  private long storedDate = -1;

  private boolean isReleased;

  private String openTrace;

  /**
   * Constructor.
   */
  public ArtifactData() {
    stats.totalAllocated++;
  }

  /**
   * Constructor for artifact data that is not (yet) part of a LOCKSS repository.
   *
   * @param artifactMetadata A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param responseStatus   A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   */
  public ArtifactData(HttpHeaders artifactMetadata, InputStream inputStream, StatusLine responseStatus) {
    this(null, artifactMetadata, inputStream, responseStatus, null, null);
  }

  /**
   * Constructor for artifact data that has an identity relative to a LOCKSS repository, but has not yet been added to
   * an artifact store.
   *
   * @param identifier       An {@code ArtifactIdentifier} for this artifact data.
   * @param artifactMetadata A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param httpStatus       A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   */
  public ArtifactData(ArtifactIdentifier identifier,
                      HttpHeaders artifactMetadata,
                      InputStream inputStream,
                      StatusLine httpStatus) {
    this(identifier, artifactMetadata, inputStream, httpStatus, null, null);
  }

  /**
   * Full constructor for artifact data.
   *
   * @param identifier       An {@code ArtifactIdentifier} for this artifact data.
   * @param artifactMetadata A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param httpStatus       A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   * @param storageUrl       A {@code String} URL pointing to the storage of this artifact data.
   * @param state     A {@code RepositoryArtifactMetadata} containing repository state information for this artifact data.
   */
  public ArtifactData(ArtifactIdentifier identifier,
                      HttpHeaders artifactMetadata,
                      InputStream inputStream,
                      StatusLine httpStatus,
                      URI storageUrl,
                      ArtifactRepositoryState state) {
    this.identifier = identifier;
    this.httpStatus = httpStatus;
    this.storageUrl = storageUrl;
    this.artifactRepositoryState = state;
    stats.totalAllocated++;

    this.setInputStream(inputStream);

    this.artifactMetadata = Objects.nonNull(artifactMetadata) ? artifactMetadata : new HttpHeaders();

    setCollectionDate(this.artifactMetadata.getDate());

  }

  /**
   * Returns additional key-value properties associated with this artifact.
   *
   * @return A {@code HttpHeaders} containing this artifact's additional properties.
   */
  public HttpHeaders getMetadata() {
    return artifactMetadata;
  }

  public void setMetadata(HttpHeaders headers) {
    this.artifactMetadata = headers;
  }

  /**
   * Returns true if an InputStream is available.
   *
   * @return true if this artifact's byte stream is available
   */
  public boolean hasContentInputStream() {
    return origInputStream != null;
  }

  /**
   * Returns true if this ArtifactData originally had an InputStream.  Used
   * for stats
   */
  public boolean hadAnInputStream() {
    return hadAnInputStream;
  }

  /** If the argument is true, a MessageDigest of the content will be
   * computed as it's read.  Must be called before {@link
   * #getInputStream()}
   * @param val If true a digest will be computed.
   */
  public void setComputeDigestOnRead(boolean val) {
    isComputeDigestOnRead = val;
  }

  /**
   * Returns this artifact's byte stream in a one-time use {@code InputStream}.
   *
   * @return An {@code InputStream} containing this artifact's byte stream.
   */
  public synchronized InputStream getInputStream() {
    // Comment in to log creation point of unused InputStreams
// 	openTrace = stackTraceString(new Exception("Open"));

    try {
      // Wrap the stream in a DigestInputStream
      if (!hadAnInputStream) {
	throw new IllegalStateException("Attempt to get InputStream from ArtifactData that was created without one");
      }
      if (origInputStream == null) {
	throw new IllegalStateException("Attempt to get InputStream from ArtifactData whose InputStream has been used");
      }
      cis = new CountingInputStream(origInputStream);
      if (isComputeDigestOnRead) {
	dis = new DigestInputStream(cis, MessageDigest.getInstance(DEFAULT_DIGEST_ALGORITHM));
	eofis = new EofRememberingInputStream(dis);
      } else {
	eofis = new EofRememberingInputStream(cis);
      }
      // Wrap the WARC-aware byte stream of this artifact so that the
      // underlying stream can be closed.
      artifactStream = new CloseCallbackInputStream(
          eofis,
          new CloseCallbackInputStream.Callback() {
            // Called when the close() method of the stream is closed.
            @Override
            public void streamClosed(Object o) {
              // Release any resources bound to this object.
              ((ArtifactData) o).release();
            }
          },
          this);
      origInputStream = null;
    } catch (NoSuchAlgorithmException e) {
      String errMsg = String.format(
          "Unknown digest algorithm: %s; could not instantiate a MessageDigest", DEFAULT_DIGEST_ALGORITHM
      );

      log.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    InputStream res = artifactStream;
    artifactStream = null;
    return res;
  }

  public void setInputStream(InputStream inputStream) {
    if (inputStream != null) {
      this.origInputStream = inputStream;
      hadAnInputStream = true;
      stats.withContent++;
    }
  }

  /**
   * Returns this artifact's HTTP response status if it originated from a web server.
   *
   * @return A {@code StatusLine} containing this artifact's HTTP response status.
   */
  public StatusLine getHttpStatus() {
    return this.httpStatus;
  }

  public void setHttpStatus(StatusLine status) {
    this.httpStatus = status;
  }

  /**
   * Return this artifact data's artifact identifier.
   *
   * @return An {@code ArtifactIdentifier}.
   */
  public ArtifactIdentifier getIdentifier() {
    return this.identifier;
  }

  /**
   * Sets an artifact identifier for this artifact data.
   *
   * @param identifier An {@code ArtifactIdentifier} for this artifact data.
   * @return This {@code ArtifactData} with its identifier set to the one provided.
   */
  public ArtifactData setIdentifier(ArtifactIdentifier identifier) {
    this.identifier = identifier;
    return this;
  }

  /**
   * Returns the repository state information for this artifact data.
   *
   * @return A {@code RepositoryArtifactMetadata} containing the repository state information for this artifact data.
   */
  public ArtifactRepositoryState getArtifactRepositoryState() {
    return artifactRepositoryState;
  }

  /**
   * Sets the repository state information for this artifact data.
   *
   * @param metadata A {@code RepositoryArtifactMetadata} containing the repository state information for this artifact.
   * @return
   */
  public ArtifactData setArtifactRepositoryState(ArtifactRepositoryState metadata) {
    this.artifactRepositoryState = metadata;
    return this;
  }

  /**
   * Returns the location where the byte stream for this artifact data can be found.
   *
   * @return A {@code String} containing the storage of this artifact data.
   */
  public URI getStorageUrl() {
    return storageUrl;
  }

  /**
   * Sets the location where the byte stream for this artifact data can be found.
   *
   * @param storageUrl A {@code String} containing the location of this artifact data.
   */
  public void setStorageUrl(URI storageUrl) {
    this.storageUrl = storageUrl;
  }

  /**
   * Implements {@code Comparable<ArtifactData>} so that sets of {@code ArtifactData} can be ordered.
   * <p>
   * There may be a better canonical order but for now, this defers to implementations of ArtifactIdentifier.
   *
   * @param other Another {@code ArtifactData} to compare against.
   * @return An {@code int} denoting the order of this artifact, relative to another.
   */
  @Override
  public int compareTo(ArtifactData other) {
    return this.getIdentifier().compareTo(other.getIdentifier());
  }

  public String getContentDigest() {
    if (contentDigest == null) {
      throw new RuntimeException("Content digest has not been set");
    }
    return contentDigest;
  }

  public void setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
  }

  public long getContentLength() {
    if (contentLength < 0) {
      throw new IllegalStateException("Content length has not been set");
    }
    return contentLength;
  }

  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  /**
   * Provides the artifact collection date.
   *
   * @return a long with the artifact collection date in milliseconds since
   * the epoch.
   */
  public long getCollectionDate() {
    return collectionDate;
  }

  /**
   * Saves the artifact collection date.
   *
   * @param collectionDate A long with the artifact collection date in milliseconds since
   *                       the epoch.
   */
  public void setCollectionDate(long collectionDate) {
    if (collectionDate >= 0) {
      this.collectionDate = collectionDate;
    }
  }

  /**
   * Returns a closable version of this artifact's byte stream.
   *
   * @return an {@code InputStream} with the underlying, closable, byte
   * stream.
   */
  public InputStream getClosableInputStream() {
    return closableInputStream;
  }

  /**
   * Sets the closable version of this artifact's byte stream.
   *
   * @param closableInputStream A {@code InputStream} containing the underlying, closable, byte
   *                            stream.
   */
  public void setClosableInputStream(InputStream closableInputStream) {
    this.closableInputStream = closableInputStream;
  }

  // Temporary until InputStream refactored
  private boolean isAutoRelease = false;

  public void setAutoRelease(boolean val) {
    isAutoRelease = val;
  }

  public boolean isAutoRelease() {
    return isAutoRelease;
  }

  /**
   * Releases resources used.
   */
  public synchronized void release() {
    if (!isReleased) {
      IOUtils.closeQuietly(closableInputStream);
      updateStats();
      artifactStream = null;
      closableInputStream = null;
      isReleased = true;
    }
  }

  @Override
  public String toString() {
    return "[ArtifactData identifier=" + identifier + ", artifactMetadata="
        + artifactMetadata + ", httpStatus=" + httpStatus
        + ", artifactRepositoryState=" + artifactRepositoryState + ", storageUrl="
        + storageUrl + ", contentDigest=" + contentDigest
        + ", contentLength=" + contentLength + ", collectionDate="
        + getCollectionDate() + "]";
  }

  public long getBytesRead() {
    if (!eofis.isAtEof()) {
      throw new RuntimeException("Content length has not been computed");
    }
    return cis.getByteCount();
  }

  public MessageDigest getMessageDigest() {
    if (dis == null) {
      throw new RuntimeException("Content digest was not requested");
    }
    if (!eofis.isAtEof()) {
      throw new RuntimeException("Content digest has not been computed");
    }
    return dis.getMessageDigest();
  }

  public String stackTraceString(Throwable th) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    th.printStackTrace(pw);
    return sw.toString();
  }

  /**
   * Finalizer.
   */
  @Override
  protected void finalize() throws Throwable {
    if (!isReleased) {
      stats.unreleased++;
      IOUtils.closeQuietly(closableInputStream);
      updateStats();
    }
    super.finalize();
  }

  private void updateStats() {
    if (hadAnInputStream) {
      if (origInputStream == null) {
	stats.inputUsed++;
      } else {
	stats.inputUnused++;
	log.debug2("Unused InputStream: {}, opened at {}",
		   getIdentifier(), openTrace);
      }
    }
  }

  public static Stats getStats() {
    return stats;
  }

  static Stats stats = new Stats();

  @Override
  public void close() throws IOException {
    if (hasContentInputStream()) {
      origInputStream.close();
      origInputStream = null;
    }

    if (closableInputStream != null) {
      closableInputStream.close();
      closableInputStream = null;
    }
  }

  public long getStoredDate() {
    return storedDate;
  }

  public void setStoredDate(long storedDate) {
    this.storedDate = storedDate;
  }

  public static class Stats {
    private volatile long totalAllocated;
    private volatile long withContent;;
    private volatile long inputUsed;
    private volatile long inputUnused;
    private volatile long unreleased;

    public long getTotalAllocated() {
      return totalAllocated;
    }

    public long getWithContent() {
      return withContent;
    }

    public long getInputUsed() {
      return inputUsed;
    }

    public long getInputUnused() {
      return inputUnused;
    }

    public long getUnreleased() {
      return unreleased;
    }
  }
}
