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

import org.lockss.log.L4JLogger;
import org.lockss.util.PreOrderComparator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VariantState {
  private final static L4JLogger log = L4JLogger.getLogger();

  //  protected List<ArtifactSpec> allSpecs = new ArrayList<>();
  protected List<ArtifactSpec> addedSpecs = new ArrayList<>();

  // Maps ArtButVer to ArtifactSpec for highest version added to the repository
  Map<String, ArtifactSpec> highestVerSpec = new HashMap<String, ArtifactSpec>();

  // Maps ArtButVer to ArtifactSpec for highest version added and committed to
  // the repository
  Map<String, ArtifactSpec> highestCommittedVerSpec = new HashMap<String, ArtifactSpec>();

  // Comparators across AUs.
  protected static final Comparator<ArtifactSpec> BY_DATE_BY_AUID_BY_DECREASING_VERSION =
      Comparator.comparing(ArtifactSpec::getCollectionDate)
          .thenComparing(ArtifactSpec::getAuid)
          .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed());

  protected static final Comparator<ArtifactSpec> BY_URI_BY_AUID_BY_DECREASING_VERSION =
      Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
          .thenComparing(ArtifactSpec::getAuid)
          .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed());

  public void add(ArtifactSpec spec) {
    addedSpecs.add(spec);

    // Remember the highest version of this URL we've added
    ArtifactSpec maxVerSpec = getHighestVerSpec(spec.artButVerKey());
    if (maxVerSpec == null || maxVerSpec.getVersion() < spec.getVersion()) {
      setHighestVerSpec(spec.artButVerKey(), spec);
    }

    // Remember the highest version of this URL we've committed, if committed
    if (spec.isCommitted) {
      commit(spec.getArtifactId());
    }
  }

  public void commit(String artifactId) {
    if (artifactExists(artifactId)) {
      ArtifactSpec spec = getArtifactSpec(artifactId);
      spec.setCommitted(true);

      ArtifactSpec maxCommittedVerSpec = getHighestCommittedVerSpec(spec.artButVerKey());
      if (maxCommittedVerSpec == null || maxCommittedVerSpec.getVersion() < spec.getVersion()) {
        setHighestCommittedVerSpec(spec.artButVerKey(), spec);
      }
    } else {
      // Artifact does not exist: Nothing to commit
    }
  }

  private boolean artifactExists(String artifactId) {
    return getArtifactSpec(artifactId) == null ? false : true;
  }

  public void addAll(Iterable<? extends ArtifactSpec> specs) {
    specs.forEach(this::add);
  }

  public ArtifactSpec getArtifactSpec(String artifactId) {
    return addedSpecStream()
        .filter(spec -> spec.getArtifactId().equals(artifactId))
        .findFirst()
        .orElse(null);
  }

  public Iterable<? extends ArtifactSpec> getArtifactSpecs() {
    return addedSpecs;
  }

  public Iterable<? extends ArtifactSpec> getDeletedSpecs() {
    return addedSpecs.stream().filter(ArtifactSpec::isDeleted).collect(Collectors.toList());
  }

  // UTILITIES

  public void logAdded() {
    for (ArtifactSpec spec : addedSpecs) {
      log.info("spec: " + spec);
    }
  }

  public long expectedVersions(ArtifactSpec spec) {
    return addedSpecStream()
        .filter(s -> spec.sameArtButVer(s))
        .count();
  }

  public List<String> activeAuids(String collectionId) {
    return addedSpecStream()
        .filter(spec -> spec.getCollection().equals(collectionId))
        .map(spec -> spec.getAuid())
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  public List<String> allAuids() {
    return addedSpecStream()
        .map(ArtifactSpec::getAuid)
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> allAuids(String collection) {
    return addedSpecStream()
        .filter(spec -> spec.getCollection().equals(collection))
        .map(ArtifactSpec::getAuid)
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> addedCommittedAuids() {
    return addedSpecStream()
        .filter(spec -> spec.isCommitted())
        .map(ArtifactSpec::getAuid)
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> addedCommittedUrls() {
    return addedSpecStream()
        .filter(spec -> spec.isCommitted())
        .map(ArtifactSpec::getUrl)
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> allCollections() {
    return collectionsOf(addedSpecStream())
        .collect(Collectors.toList());
  }

  public List<String> activeCollections() {
    return collectionsOf(addedSpecStream()
        .filter(spec -> !spec.isDeleted()))
        .collect(Collectors.toList());
  }

  public List<String> activeCommittedCollections() {
    return collectionsOf(addedSpecStream()
        .filter(spec -> !spec.isDeleted() && spec.isCommitted()))
        .collect(Collectors.toList());
  }

  Stream<String> collectionsOf(Stream<ArtifactSpec> specStream) {
    return specStream
        .map(ArtifactSpec::getCollection)
        .distinct();
  }

  Stream<String> auidsOf(Stream<ArtifactSpec> specStream, String collection) {
    return specStream
        .filter(s -> s.getCollection().equals(collection))
        .map(ArtifactSpec::getAuid)
        .distinct();
  }

  public Stream<ArtifactSpec> addedSpecStream() {
    return addedSpecs.stream();
  }

  public Stream<ArtifactSpec> committedSpecStream() {
    return addedSpecStream().filter(spec -> spec.isCommitted());
  }

  public Stream<ArtifactSpec> deletedSpecStream() {
    return addedSpecStream().filter(ArtifactSpec::isDeleted);
  }

  Stream<ArtifactSpec> uncommittedSpecStream() {
    return addedSpecStream().filter(spec -> !spec.isCommitted());
  }

  public Stream<ArtifactSpec> orderedAll() {
    return addedSpecStream()
        .sorted();
  }

  public Stream<ArtifactSpec> orderedAllCommitted() {
    return committedSpecStream()
        .sorted();
  }

  public Stream<ArtifactSpec> orderedAllCommittedAllAus() {
    return committedSpecStream()
        .sorted(BY_DATE_BY_AUID_BY_DECREASING_VERSION);
  }

  public Stream<ArtifactSpec> orderedAllCollIncludeUncommitted(String coll) {
    return addedSpecStream()
        .filter(s -> s.getCollection().equals(coll))
        .sorted();
  }

  public Stream<ArtifactSpec> orderedAllColl(String coll) {
    return committedSpecStream()
        .filter(s -> s.getCollection().equals(coll))
        .sorted();
  }

  public Stream<ArtifactSpec> orderedAllCollAllAus(String coll) {
    return committedSpecStream()
        .filter(s -> s.getCollection().equals(coll))
        .sorted(BY_URI_BY_AUID_BY_DECREASING_VERSION);
  }

  public Stream<ArtifactSpec> orderedAllAu(String coll, String auid) {
    return committedSpecStream()
        .filter(s -> s.getCollection().equals(coll))
        .filter(s -> s.getAuid().equals(auid))
        .sorted();
  }

  // TODO: Return AuSize
  public long auSize(String collection, String auid) {
    return orderedAllAu(collection, auid).mapToLong(ArtifactSpec::getContentLength).sum();
  }

  Stream<ArtifactSpec> orderedAllUrl(String coll, String auid, String url) {
    return committedSpecStream()
        .filter(s -> s.getCollection().equals(coll))
        .filter(s -> s.getAuid().equals(auid))
        .filter(s -> s.getUrl().equals(url))
        .sorted();
  }

  public ArtifactSpec anyCommittedSpec() {
    return committedSpecStream().findAny().orElse(null);
  }

  public ArtifactSpec anyDeletedSpec() {
    return deletedSpecStream().findAny().orElse(null);
  }

  public ArtifactSpec anyUncommittedSpec() {
    return uncommittedSpecStream()
        .filter(s->!s.isDeleted())
        .findAny()
        .orElse(null);
  }

  public ArtifactSpec anyUncommittedSpecButVer() {
    return uncommittedSpecStream()
        .filter(spec -> !highestCommittedVerSpec.containsKey(spec.artButVerKey()))
        .findAny().orElse(null);
  }

  // Return the highest version ArtifactSpec with same ArtButVer
  ArtifactSpec highestVer(ArtifactSpec likeSpec, Stream<ArtifactSpec> stream) {
    return stream
        .filter(spec -> spec.sameArtButVer(likeSpec))
        .max(Comparator.comparingInt(ArtifactSpec::getVersion))
        .orElse(null);
  }

  // Delete ArtifactSpec from record of what we've added to the repository,
  // adjust highest version maps accordingly
  public void delFromAll(ArtifactSpec spec) {
    addedSpecs.remove(spec);
    String key = spec.artButVerKey();
    if (highestVerSpec.get(key) == spec) {
      ArtifactSpec newHigh = highestVer(spec, addedSpecStream());
      log.info("newHigh: " + newHigh);
      highestVerSpec.put(key, newHigh);
    }
    if (highestCommittedVerSpec.get(key) == spec) {
      ArtifactSpec newCommHigh = highestVer(spec, committedSpecStream());
      log.info("newCommHigh: " + newCommHigh);
      highestCommittedVerSpec.put(key, newCommHigh);
    }
  }

  public Collection<ArtifactSpec> getHighestCommittedVerSpecs() {
    return highestCommittedVerSpec.values();
  }

  public Collection<ArtifactSpec> getHighestVerSpecs() {
    return highestVerSpec.values();
  }

  public ArtifactSpec getHighestCommittedVerSpec(String artButVerKey) {
    return highestCommittedVerSpec.get(artButVerKey);
  }

  public ArtifactSpec getHighestVerSpec(String artButVerKey) {
    return highestVerSpec.get(artButVerKey);
  }

  public void setHighestVerSpec(String artButVerKey, ArtifactSpec spec) {
    highestVerSpec.put(artButVerKey, spec);
  }

  public void setHighestCommittedVerSpec(String artButVerKey, ArtifactSpec spec) {
    highestCommittedVerSpec.put(artButVerKey, spec);
  }

  public boolean hasHighestCommittedVerSpec(String artButVerKey) {
    return highestCommittedVerSpec.containsKey(artButVerKey);
  }

  public Stream<ArtifactSpec> getArtifactsAllVersions(String collection, String auid, boolean includeUncommitted) {
    return addedSpecStream()
        .filter(s -> s.getCollection().equals(collection))
        .filter(s -> s.getAuid().equals(auid))
        .filter(s -> includeUncommitted || s.isCommitted())
        .filter(s -> !s.isDeleted())
        .sorted();
  }

  public List<Artifact> getArtifactsFrom(Stream<ArtifactSpec> specStream) {
    return specStream.map(s -> s.getArtifact()).collect(Collectors.toList());
  }

  public ArtifactSpec getLatestArtifactSpec(String collection, String auid, String uri, boolean includeUncommitted) {
    return getArtifactsAllVersions(collection, auid, includeUncommitted)
        .filter(s -> s.getUrl().equals(uri))
        .max(Comparator.comparingInt(ArtifactSpec::getVersion)).orElse(null);
  }

  public Stream<ArtifactSpec> getLatestArtifactSpecs(String collection, String auid, boolean includeUncommitted) {
    return getHighestVerSpecs().stream()
        .filter(spec -> spec.getCollection().equals(collection))
        .filter(spec -> spec.getAuid().equals(auid))
        .filter(s -> includeUncommitted || s.isCommitted())
        .sorted();
  }
}
