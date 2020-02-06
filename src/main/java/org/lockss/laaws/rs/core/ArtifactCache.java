/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.laaws.rs.core;

import java.util.*;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.map.*;
import org.apache.commons.lang3.StringUtils;

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.log.L4JLogger;


/**
 * Caches recently referenced Artifacts.  Artifacts are keyed by
 * (collection, auid, url, version).  The latest version, when known, is
 * stored under the key (collection, auid, url, -1).  Two caches are
 * employed: one for requests for a specific Artifact version, the other
 * for the results of iterations.  Otherwise, every non-trivial iteration
 * would flush all other Artifacts from the cache.  If an Artifact
 * resulting from an iteration is going to be re-accessed it will likely be
 * very soon.
 *
 * Cached values are invalidated upon receipt of a JMS message from the
 * repository service.  No attempt is made to ensure Artifact uniqueness;
 * it's unnecessary as Artifacts are immutable (or soon will be), and
 * LockssRepository doesn't guarantee uniqueness anyway.
 *
 * Separately caches recently used ArtifactData, keyed by collection +
 * artifactId.
 */
public class ArtifactCache {
  private final static L4JLogger log = L4JLogger.getLogger();

  int maxArtSize;
  int maxArtDataSize;

  // Artifact key -> Artifact
  LRUMap<String,Artifact> artMap;
  LRUMap<String,Artifact> artIterMap;

  // Artifact ID -> ArtifactData
  LRUMap<String,ArtifactData> artDataMap;

  Stats stats = new Stats();
  private boolean isInstrumented = false;

  /** Create and enable the cache */
  public ArtifactCache(int maxArtSize, int maxArtDataSize) {
    this.maxArtSize = maxArtSize;
    this.maxArtDataSize = maxArtDataSize;
    enable(true);
  }

  /** Enable or disable the cache.
   * @param enable true to enable
   * @return this
   */
  public synchronized ArtifactCache enable(boolean enable) {
    if (enable) {
      if (artMap == null) {
	artMap = new LRUMap<>(maxArtSize);
	artIterMap = new LRUMap<>(maxArtSize);
	artDataMap = new ArtifactDataReleasingLRUMap(maxArtDataSize);
	stats.setSizes(maxArtSize, maxArtDataSize);
      }
    } else {
      artMap = null;
      artIterMap = null;
      artDataMap = null;
    }
    return this;
  }

  /** Return true if the cache is enabled.
   * @return true if the cache is enabled.
   */
  public boolean isEnabled() {
    return artMap != null;
  }

  /** Enable LRUMap usage histograms, which show the recency of cache hit
   * items but take extra time to compute at each access */
  public void enableInstrumentation(boolean enable) {
    isInstrumented = enable;
  }

  public boolean isInstrumented() {
    return isInstrumented;
  }

  /** Internal get; return the Artifact stored under the key, or null if
   * none */
  private synchronized Artifact get(String key) {
    if (artMap == null) return null;
    if (!updateHist(stats.artHist, artMap, key)) {
      updateHist(stats.artIterHist, artIterMap, key);
    }
    boolean isIterMap = false;
    Artifact res = artMap.get(key);
    if (res == null) {
      res = artIterMap.get(key);;
      isIterMap = true;
    }
    if (res == null) {
      stats.cacheMisses++;
    } else if (isIterMap) {
      stats.cacheIterHits++;
      log.trace("get({} (iter)): {}", key, res);
    } else {
      stats.cacheHits++;
      log.trace("get({}): {}", key, res);
    }
    return res;
  }

  /** Return a cached Artifact with the same key as protoArt.
   * @param protoArt model for the cached Artifact to retrieve
   * @return cached Artifact or null if not found in cache.
   */
  public Artifact get(Artifact protoArt) {
    return get(protoArt.makeKey());
  }

  /** Return a cached latest-version Artifact with the collection, auid and
   * url.
   * @param collection
   * @param auid
   * @param url
   * @return cached latest version of specified Artifact or null if not
   * found in cache.
   */
  public Artifact getLatest(String collection, String auid, String url) {
    // "latest"
    return get(Artifact.makeLatestKey(collection, auid, url));
  }

  /** Return a cached Artifact with the collection, auid, url and version.
   * @param collection
   * @param auid
   * @param url
   * @param version
   * @return cached Artifact or null if not found in cache.
   */
  public Artifact get(String collection, String auid, String url, int version) {
    return get(Artifact.makeKey(collection, auid, url, version));
  }

  /** Internal store
   * @param key Artifact key
   * @param art Artifact
   * @return the Artifact
   */
  private synchronized Artifact put(String key, Artifact art) {
    if (artMap == null) {
      return art;
    }
    // If it's already in the iter map, put new one there.  (Not checking
    // for this would defeat having a separate iter map in any case where
    // an iteration calls getArtifact() on each Artifact returned by the
    // iterator.)
    if (artIterMap.containsKey(key)) {
      artIterMap.put(key, art);
    } else {
      artMap.put(key, art);
    }
    log.trace("put({}, {})", key, art);
    stats.cacheStores++;
    return art;
  }

  /** Store the Artifact in the cache.
   * @param art Artifact
   * @return the Artifact
   */
  public Artifact put(Artifact art) {
    return put(art.makeKey(), art);
  }

  /** Store the Artifact in the cache.
   * @param art Artifact
   * @return the Artifact
   */
  public synchronized Artifact putLatest(Artifact art) {
    if (artMap == null) return art;
    if (!art.getCommitted()) {
      throw new IllegalStateException("putLatest() called with uncommitted Artifact: " + art);
    }
    String latestKey = art.makeLatestKey();
    if (artIterMap.containsKey(latestKey)) {
      artIterMap.put(latestKey, art);
    } else {
      artMap.put(latestKey, art);
    }
    // Store under version
    put(art);
    return art;
  }

  /** Store the Artifact in the iterator cache.
   * @param art Artifact
   * @return the Artifact
   */
  public synchronized Artifact putIterLatest(Artifact art) {
    if (artMap == null) return art;
    if (!art.getCommitted()) {
      throw new IllegalStateException("putLatest() called with uncommitted Artifact: " + art);
    }
    String key = art.makeKey();
    String latestKey = art.makeLatestKey();
    // if already in artMap, leave it there,
    if (artMap.containsKey(key)) {
      artMap.put(key, art);
      artMap.put(latestKey, art);
      artIterMap.remove(key);
      artIterMap.remove(latestKey);
    } else {
      artIterMap.put(key, art);
      artIterMap.put(latestKey, art);
    }
    return art;
  }

  /** Delete all cache entries. */
  public synchronized void flush() {
    if (artMap == null) return;
    artMap.clear();
    artIterMap.clear();
    stats.cacheFlushes++;
  }

  public enum InvalidateOp {Commit, Delete};

  /** Remove Artifact from the cache.
   * @param op an InvalidateOp reflecting the operation that caused the
   * invalidation.  (Not currently used.)
   * @param key the artifact key
   */
  public synchronized void invalidate(InvalidateOp op, String key) {
    if (artMap == null) return;
    String latestKey = Artifact.makeLatestKey(key);

    // Be conservative and always remove the latest version of this
    // artifact.  The circumstances in which this could unnecessarily
    // remove a latest version are quite rare (delete, commit non-latest
    // version) so it's not worth detecting the few cases where we can
    // safely determine that it's not necessary.

    if (artMap.containsKey(key) || artMap.containsKey(latestKey) ||
	artIterMap.containsKey(key) || artIterMap.containsKey(latestKey)) {
      artMap.remove(key);
      artMap.remove(latestKey);
      artIterMap.remove(key);
      artIterMap.remove(latestKey);
      stats.cacheInvalidates++;
    }
  }

  /** Wrap an artifact iterator so that each Artifact it returns is added
   * to the cache as "latest".  Should only be used when it is known that
   * all the Artifacts returned by the underlying iterator are latest.
   * @param iter an Iterator<Artifact>
   * @return a wrapped Iterator<Artifact>
   */
  public Iterator<Artifact> cachingLatestIterator(Iterator<Artifact> iter) {
    return new CachingIterator(iter, true);
  }

  /** Iterator wrapper which stores Artifacts in the cache as they're
   * returned. */
  public class CachingIterator implements Iterator<Artifact> {
    Iterator<Artifact> iter;
    boolean isLatestOnly;

    public CachingIterator(Iterator<Artifact> iter) {
      this(iter, false);
    }

    /** Create a wrapped Iterator.
     * @param iter the Iterator to wrap
     * @param isLatestOnly if true, the returned artifacts will be cached
     * as latest version; if false they will be cached only under their
     * exact version
     */
    public CachingIterator(Iterator<Artifact> iter, boolean isLatestOnly) {
      this.iter = iter;
      this.isLatestOnly = isLatestOnly;
    }

    public boolean hasNext() {
      return iter.hasNext();
    }

    public Artifact next() {
      Artifact art = iter.next();
      if (isLatestOnly) {
	putIterLatest(art);
      }
      return art;
    }
  }

  /** For now, ArtifactData.release() is called when items exit the
   * cache */
  static class ArtifactDataReleasingLRUMap
    extends LRUMap<String, ArtifactData> {

    ArtifactDataReleasingLRUMap(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean removeLRU(LinkEntry<String, ArtifactData> entry) {
      ArtifactData ad = entry.getValue();
      ad.release();
      return true;
    }
  }

  /**
   * Sets max cache size.
   * @param newArtSize the new size for the Artifact cache
   * @param newArtDataSize the new size for the ArtifactData cache
   */
  public synchronized void setMaxSize(int newArtSize, int newArtDataSize) {
    if (newArtSize<=0) {
      throw new IllegalArgumentException("Negative cache size");
    }
    if (artMap != null) {
      if (artMap.maxSize() != newArtSize) {
	LRUMap<String,Artifact> newMap = new LRUMap<>(newArtSize);
	newMap.putAll(artMap);
	artMap = newMap;
	stats.setArtSize(newArtSize);
	LRUMap<String,Artifact> newIterMap = new LRUMap<>(newArtSize);
	newIterMap.putAll(artIterMap);
	artIterMap = newIterMap;
      }
      if (artDataMap.maxSize() != newArtDataSize) {
	LRUMap<String,ArtifactData> newDataMap = new LRUMap<>(newArtDataSize);
	newDataMap.putAll(artDataMap);
	artDataMap = newDataMap;
	stats.setArtDataSize(newArtDataSize);
      }
    }
    // Set these even if diabled, so they're right when enabled
    maxArtSize = newArtSize;
    maxArtDataSize = newArtDataSize;
  }

  // ArtifactData cache

  /** Store the ArtifactData in the cache.  If there is already one there,
   * do not replace it if it has a content InputStream and the new one
   * doesn't.
   * @param collection
   * @param artifactId
   * @return the ArtifactData that is now in the cache
   */
  public synchronized ArtifactData putArtifactData(String collection,
						   String artifactId,
						   ArtifactData ad) {
    if (artDataMap == null) {
      return ad;
    }
    String key = artifactDataKey(collection, artifactId);
    ArtifactData old = artDataMap.get(key);
    if (old != null && old.hasContentInputStream()
	&& !ad.hasContentInputStream()) {
      log.trace("Not replacing unused ArtifactData with a used one: {}", key);
      return old;
    }
    artDataMap.put(key, ad);
    ad.setAutoRelease(true);
    log.trace("putArtifactData({}, {})", key, ad);
    stats.dataCacheStores++;
    return ad;
  }

  /** Return a cached ArtifactData
   * @param collection
   * @param artifactId
   * @param needInputStream if true, a cached item will be returned only if
   * it has an unused InputStream
   * @return cached ArtifactDate or null if not found or if an InputStream
   * is requred and not available.
   */
  public synchronized ArtifactData getArtifactData(String collection,
						   String artifactId,
						   boolean needInputStream) {
    if (artDataMap == null) return null;
    String key = artifactDataKey(collection, artifactId);
    updateHist(stats.artDataHist, artDataMap, key);
    ArtifactData res = artDataMap.get(key);
    if (res != null && needInputStream && !res.hasContentInputStream()) {
      res = null;
    }
    if (res == null) {
      stats.dataCacheMisses++;
    } else {
      stats.dataCacheHits++;
      log.trace("getArtifactData({}): {}", key, res);
    }
    return res;
  }

  private static String artifactDataKey(String collection, String artifactId) {
    return collection + "|" + artifactId;
  }

  /** Update the histogram of positions in the cache where a hit was found.
   * This must be called *before* the item is looked up, as that operation
   * moves it to the LRU position.
   * @param hist the history array to update
   * @param the LRU map in question
   * @param the key being looked up in the map
   * @return true iff the key was found an the historgram updated
   */
  private boolean updateHist(int[] hist,
			     LRUMap<String,? extends Object> map,
			     String key) {
    if (isInstrumented()) {
      if (map.containsKey(key)) {
	int ix = 0;
	String mapkey = map.lastKey();
	do {
	  if (mapkey.equals(key)) {
	    updateHist(hist, map.maxSize(), ix);
	    return true;
	  }
	  ix++;
	} while ((mapkey = map.previousKey(mapkey)) != null);
      }
    }
    return false;
  }

  private void updateHist(int[] hist, int max, int val) {
    int ix = val * hist.length / max;
    log.trace("updateHist({}, {}): {}", max, val, ix);
    hist[ix]++;
  }

  /** Return a Stats object describing the hit & miss statistics, and
   * histograms to guide in sizing the caches */
  public Stats getStats() {
    return stats;
  }

  public static class Stats {
    private int maxArtSize = 0;
    private int cacheHits = 0;
    private int cacheIterHits = 0;
    private int cacheMisses = 0;
    private int cacheStores = 0;
    private int cacheInvalidates = 0;
    private int cacheFlushes = 0;
    private int[] artHist;
    private int[] artIterHist;

    private int maxArtDataSize = 0;
    private int dataCacheHits = 0;
    private int dataCacheMisses = 0;
    private int dataCacheStores = 0;
    private int[] artDataHist;

    public void setSizes(int maxArtSize, int maxArtDataSize) {
      setArtSize(maxArtSize);
      setArtDataSize(maxArtDataSize);
    }

    public void setArtSize(int maxArtSize) {
      artHist = new int[Math.min(maxArtSize, 20)];
      artIterHist = new int[Math.min(maxArtSize, 20)];
      this.maxArtSize = maxArtSize;
    }

    public void setArtDataSize(int maxArtDataSize) {
      artDataHist = new int[Math.min(maxArtDataSize, 20)];
      this.maxArtDataSize = maxArtDataSize;
    }

    public int getCacheHits() {
      return cacheHits;
    }

    public int getCacheIterHits() {
      return cacheIterHits;
    }

    public int getCacheMisses() {
      return cacheMisses;
    }

    public int getCacheStores() {
      return cacheStores;
    }

    public int getCacheInvalidates() {
      return cacheInvalidates;
    }

    public int getCacheFlushes() {
      return cacheFlushes;
    }

    public int getDataCacheHits() {
      return dataCacheHits;
    }

    public int getDataCacheMisses() {
      return dataCacheMisses;
    }

    public int getDataCacheStores() {
      return dataCacheStores;
    }

    public int getMaxArtSize() {
      return maxArtSize;
    }

    public int getMaxArtDataSize() {
      return maxArtDataSize;
    }

    public int[] getArtHist() {
      return artHist;
    }

    public int[] getArtIterHist() {
      return artIterHist;
    }

    public int[] getArtDataHist() {
      return artDataHist;
    }
  }

  // for unit tests

  boolean containsKey(String key) {
    return artMap.containsKey(key);
  }

  boolean containsIterKey(String key) {
    return artIterMap.containsKey(key);
  }

}
