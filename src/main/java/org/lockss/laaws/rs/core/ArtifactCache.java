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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.map.*;
import org.apache.commons.lang3.StringUtils;

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.log.L4JLogger;

// Invalidate ops

// commit - invalidate version, invalidate latest version if version >=
// latest version
// delete - invalidate version, invalidate latest if == version
// add - no


/**
 * Caches recently referenced Artifacts.  Artifacts are keyed by
 * (collection, auid, url, version).  The latest version, when known, is
 * stored under the key (collection, auid, url, -1).  Cached values are
 * invalidated upon receipt of a JMS message from the repository service.
 * No attempt is made to ensure Artifact uniqueness; it's unnecessary as
 * Artifacts are immutable (or soon will be), and LockssRepository doesn't
 * guarantee uniqueness anyway.
 */
public class ArtifactCache {
  private final static L4JLogger log = L4JLogger.getLogger();

  static boolean PARANOID = true;

  int maxSize;
  LRUMap<String,Artifact> artMap;

  // logging variables
  private int cacheHits = 0;
  private int cacheMisses = 0;
  private int cacheStores = 0;
  private int cacheInvalidates = 0;

  public ArtifactCache(int maxSize) {
    this.maxSize = maxSize;
    enable(true);
  }

  public ArtifactCache enable(boolean val) {
    if (val) {
      if (artMap == null) {
	artMap = new LRUMap<>(maxSize);
      }
    } else {
      artMap = null;
    }
    return this;
  }

  public boolean isEnabled() {
    return artMap != null;
  }

  private synchronized Artifact get(String key) {
    if (artMap == null) return null;
    Artifact res = artMap.get(key);
    if (res == null) {
      cacheMisses++;
    } else {
      cacheHits++;
    }
    log.trace("get({}): {}", key, res);
    return res;
  }

  public Artifact get(Artifact protoArt) {
    return get(protoArt.makeKey());
  }

  public Artifact get(String collection, String auid, String url) {
    // "latest"
    return get(Artifact.makeLatestKey(collection, auid, url));
  }

  public Artifact get(String collection, String auid, String url, int version) {
    return get(Artifact.makeKey(collection, auid, url, version));
  }

  private synchronized Artifact put(String key, Artifact art) {
    if (artMap == null) return art;
    artMap.put(key, art);
    log.trace("put({}, {})", key, art);
    cacheStores++;
    // If a latest version of this artifact is present
    String latestKey = art.makeLatestKey();
    Artifact latest = artMap.get(latestKey);
//     if (latest != null &&
// 	latest != art &&
// 	art.getCommitted() &&
// 	art.getVersion() >= latest.getVersion()) {
//       if (PARANOID) {
// 	artMap.remove(latestKey);
// 	log.trace("remove({})", latestKey);
//       } else {
// 	artMap.put(latestKey, art);
// 	log.trace("putLatest({}, {})", key, art);
//       }
//     }
    return art;
  }

  public Artifact put(Artifact art) {
    return put(art.makeKey(), art);
  }

  public synchronized Artifact putLatest(Artifact art) {
    if (artMap == null) return art;
    if (!art.getCommitted()) {
      throw new IllegalStateException("putLatest() called with uncommitted Artifact: " + art);
    }

    // Store under version
    String key = art.makeKey();
    Artifact verArt = put(key, art);
    log.trace("put({}, {})", key, art);

    // Store under latest
    String latestKey = art.makeLatestKey();
    artMap.put(latestKey, art);
    log.trace("putLatest({}, {})", latestKey, art);

    return verArt;
  }

  public enum InvalidateOp {Commit, Delete};

  /** Remove Artifact from the cache  */
  public synchronized void invalidate(InvalidateOp op, String key) {
    if (artMap == null) return;
    Artifact verArt = artMap.get(key);
    if (verArt != null) {
      artMap.remove(key);
      String latestKey = verArt.makeLatestKey();
      Artifact latestArt = artMap.get(latestKey);
      if (latestArt != null) {
	switch (op) {
	case Commit:
	  // remove cached latest if newly commited art has greater or
	  // equal version
	  if (verArt.getVersion() >= latestArt.getVersion()) {
	    artMap.remove(latestKey);
	  }
	  break;
	case Delete:
	  // remove cached latest if same as the one deleted
	  if (verArt.getVersion().equals(latestArt.getVersion())) {
	    artMap.remove(latestKey);
	  }
	  break;
	}
      }
    }
  }
      
//   /** Remove Artifact from the cache  */
//   public synchronized void invalidate(Artifact art) {
//     if (artMap == null) return;
//     String verKey = art.makeKey();
//     Artifact verArt = artMap.get(verKey);
//     if (verArt != null) {
//       artMap.remove(verKey);
//       removeIfLatest(verArt);
//     }
//   }

  private void removeIfLatest(Artifact art) {
    // if this is the latest, remove that too
    String latestKey = art.makeLatestKey();
    Artifact latestArt = artMap.get(latestKey);
    if (latestArt != null) {
      artMap.remove(latestKey);
    }
  }

  // logging accessors

  public int getCacheHits() { return cacheHits; }
  public int getCacheMisses() { return cacheMisses; }
  public int getCacheStores() { return cacheStores; }
  public int getCacheInvalidates() { return cacheInvalidates; }
  

//   public class InvalidatingIterator implements Iterator<Artifact> {

//     public boolean hasNext() {
//       return uter.hasNext();
//     }

//     public Artifact next() {
//       Artifact art = iter.next();
//       invalidate(art);
//       return art;
//     }
//   }
}
