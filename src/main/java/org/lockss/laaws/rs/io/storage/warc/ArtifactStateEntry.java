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

package org.lockss.laaws.rs.io.storage.warc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.AuJournalEntry;
import org.lockss.util.time.TimeBase;

import java.util.Objects;

/**
 * Encapsulates the LOCKSS repository -specific metadata of an artifact. E.g., whether an artifact is committed.
 */
@JsonAutoDetect(isGetterVisibility = JsonAutoDetect.Visibility.NONE)
public class ArtifactStateEntry implements AuJournalEntry {
  public static String LOCKSS_JOURNAL_ID = "artifact_state";

  private String artifactId;
  private long entryDate;
  private ArtifactState state;

  public ArtifactStateEntry() {

  }

  /**
   * Constructor.
   *
   * @param artifactId An {@link ArtifactIdentifier} for this journal entry.
   * @param state The {@link ArtifactState} of this artifact.
   */
  public ArtifactStateEntry(ArtifactIdentifier artifactId, ArtifactState state) {
    this.artifactId = artifactId.getId();
    this.entryDate = TimeBase.nowMs();
    this.state = state;
  }

  /**
   * Returns the metadata ID for this class of metadata.
   *
   * @return A {@code String} containing the metadata ID.
   */
  public static String getJournalId() {
    return LOCKSS_JOURNAL_ID;
  }

  /**
   * Returns the artifact ID this metadata belongs to.
   *
   * @return ArtifactData ID
   */
  @Override
  public String getArtifactId() {
    return artifactId;
  }

  @Override
  public long getEntryDate() {
    return this.entryDate;
  }

  /**
   * @param time
   * @return
   */
  public ArtifactStateEntry setEntryDate(long time) {
    this.entryDate = time;
    return this;
  }

  /**
   * Returns the state of this artifact in this entry.
   *
   * @return An {@link ArtifactState} enum with the state of the artifact.
   */
  public ArtifactState getArtifactState() {
    return this.state;
  }

  /**
   * Sets the state of this artifact.
   *
   * @param state
   * @return This {@link ArtifactStateEntry} object.
   */
  public ArtifactStateEntry setArtifactState(ArtifactState state) {
    this.state = state;
    return this;
  }

  /**
   * Returns a {@code boolean} indicating whether the state in this entry is {@code COMMITTED}.
   */
  public boolean isCommitted() {
    return this.state == ArtifactState.PENDING_COPY ||
           this.state == ArtifactState.COPIED;
  }

  /**
   * Returns a {@code boolean} indicating whether the state in this entry is {@code COPIED}.
   */
  public boolean isCopied() {
    return this.state == ArtifactState.COPIED;
  }

  /**
   * Returns a {@code boolean} indicating whether the state in this entry is {@code DELETED}.
   */
  public boolean isDeleted() {
    return this.state == ArtifactState.DELETED;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArtifactStateEntry that = (ArtifactStateEntry) o;
    return entryDate == that.entryDate && Objects.equals(artifactId, that.artifactId) && state == that.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactId, entryDate, state);
  }
}
