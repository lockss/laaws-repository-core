/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.laaws.rs.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.http.StatusLine;
import org.lockss.laaws.rs.io.storage.warc.ArtifactState;
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
  private HttpHeaders httpHeaders;
  private StatusLine httpStatus;
  private InputStream origInputStream;
  private boolean inputStreamUsed = false;
  private boolean hadAnInputStream = false;
  private long contentLength = -1;
  private String contentDigest;

  // Internal repository state
  private ArtifactState artifactState = null;
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
   * @param httpHeaders A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param responseStatus   A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   */
  public ArtifactData(HttpHeaders httpHeaders, InputStream inputStream, StatusLine responseStatus) {
    this(null, httpHeaders, inputStream, responseStatus, null, null);
  }

  /**
   * Constructor for artifact data that has an identity relative to a LOCKSS repository, but has not yet been added to
   * an artifact store.
   *
   * @param identifier       An {@code ArtifactIdentifier} for this artifact data.
   * @param httpHeaders A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param httpStatus       A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   */
  public ArtifactData(ArtifactIdentifier identifier,
                      HttpHeaders httpHeaders,
                      InputStream inputStream,
                      StatusLine httpStatus) {
    this(identifier, httpHeaders, inputStream, httpStatus, null, null);
  }

  /**
   * Full constructor for artifact data.
   *
   * @param identifier       An {@code ArtifactIdentifier} for this artifact data.
   * @param httpHeaders A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
   * @param inputStream      An {@code InputStream} containing the byte stream of this artifact.
   * @param httpStatus       A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
   * @param storageUrl       A {@code String} URL pointing to the storage of this artifact data.
   * @param state     A {@code RepositoryArtifactMetadata} containing repository state information for this artifact data.
   */
  public ArtifactData(ArtifactIdentifier identifier,
                      HttpHeaders httpHeaders,
                      InputStream inputStream,
                      StatusLine httpStatus,
                      URI storageUrl,
                      ArtifactState state) {
    this.identifier = identifier;
    this.httpStatus = httpStatus;
    this.storageUrl = storageUrl;
    this.artifactState = state;
    stats.totalAllocated++;

    this.setInputStream(inputStream);

    this.httpHeaders = Objects.nonNull(httpHeaders) ? httpHeaders : new HttpHeaders();

    setCollectionDate(this.httpHeaders.getDate());
  }

  /**
   * Returns additional key-value properties associated with this artifact.
   *
   * @return A {@code HttpHeaders} containing this artifact's additional properties.
   */
  public HttpHeaders getHttpHeaders() {
    return httpHeaders;
  }

  public void setHttpHeaders(HttpHeaders headers) {
    this.httpHeaders = headers;
  }

  /**
   * Returns true if an InputStream is available.
   *
   * @return true if this artifact's byte stream is available
   */
  public boolean hasContentInputStream() {
    return origInputStream != null && !inputStreamUsed;
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
      if (inputStreamUsed) {
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
      inputStreamUsed = true;
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

  /**
   * @return Returns a boolean indicating whether this artifact has an HTTP status (was therefore from a web crawl).
   */
  public boolean isHttpResponse() {
    return httpStatus != null;
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
  public ArtifactState getArtifactState() {
    return artifactState;
  }

  /**
   * Sets the repository state information for this artifact data.
   *
   * @param state A {@link ArtifactState} representing the state of this artifact.
   * @return This {@link ArtifactData}.
   */
  public ArtifactData setArtifactState(ArtifactState state) {
    this.artifactState = state;
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

  public boolean hasContentLength() {
    return !(contentLength < 0);
  }

  public void setContentLength(long contentLength) {
    if (contentLength < 0) {
      throw new IllegalArgumentException("Invalid content length: " + contentLength);
    }
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
    return "[ArtifactData identifier=" + identifier + ", httpHeaders="
        + httpHeaders + ", httpStatus=" + httpStatus
        + ", artifactState=" + artifactState + ", storageUrl="
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
