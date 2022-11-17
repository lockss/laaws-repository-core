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
import org.junit.jupiter.api.*;
import org.apache.commons.collections4.IteratorUtils;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.*;
import org.lockss.util.test.LockssTestCase5;

public class TestArtifactCache extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  static String NS1 = "ns1";
  static String NS2 = "ns2";
  static String AUID1 = "AAUU1";
  static String AUID2 = "AAUU2";
  static String ARTID1 = "1a-2b-3c";
  static String ARTID2 = "2b-1a-3c";
  static String ARTID3 = "1a-3c-2b";

  static String URL1 = "u1";
  static String URL2 = "u2";
  static String URL3 = "u3";

  private ArtifactCache cache;

  @BeforeEach
  public void setUp() throws Exception {
    cache = new ArtifactCache(10, 10);
    cache.enableInstrumentation(true);
  }

  Artifact makeArt(String ns, String auid, String url, int version) {
    return new Artifact("aidid", ns, auid, url, version, Boolean.TRUE,
			"store_url", 123, null);
  }

  @Test
  public void testArtCache() throws Exception {
    Artifact u1v1 = makeArt(NS1, AUID1, URL1, 1);
    Artifact u2v1 = makeArt(NS1, AUID1, URL2, 1);
    Artifact u2v2 = makeArt(NS1, AUID1, URL2, 2);
    Artifact u3v1 = makeArt(NS1, AUID1, URL3, 1);
    assertNull(cache.get(u1v1));
    ArtifactCache.Stats stats = cache.getStats();
    assertEquals(0, stats.getCacheHits());
    assertEquals(1, stats.getCacheMisses());

    assertSame(u1v1, cache.put(u1v1));
    assertSame(u1v1, cache.get(u1v1));
    assertSame(u1v1, cache.get(makeArt(NS1, AUID1, URL1, 1)));

    assertArrayEquals(new int[] {2,0,0,0,0,0,0,0,0,0},
		      cache.getStats().getArtHist());
    assertEquals(2, stats.getCacheHits());
    assertEquals(1, stats.getCacheMisses());

    assertNull(cache.get(makeArt(NS1, AUID1, URL1, -1)));
    assertNull(cache.getLatest(NS1, AUID1, URL1));
    assertSame(u2v1, cache.putLatest(u2v1));
    assertSame(u2v1, cache.get(u2v1));
    assertSame(u2v1, cache.get(makeArt(NS1, AUID1, URL2, 1)));
    assertSame(u2v1, cache.get(NS1, AUID1, URL2, -1));
    assertSame(u2v1, cache.getLatest(NS1, AUID1, URL2));
    assertEquals(-1, (int)makeArt(NS1, AUID1, URL2, -1).getVersion());
    assertSame(u2v1, cache.get(makeArt(NS1, AUID1, URL2, -1)));
    assertEquals(7, stats.getCacheHits());
    assertEquals(3, stats.getCacheMisses());

    assertSame(u2v2, cache.putLatest(u2v2));
    assertSame(u2v2, cache.get(NS1, AUID1, URL2, -1));
    assertSame(u2v2, cache.getLatest(NS1, AUID1, URL2));
    assertSame(u2v2, cache.get(NS1, AUID1, URL2, 2));
    assertSame(u2v1, cache.get(NS1, AUID1, URL2, 1));
    log.trace("arthist: {}", Arrays.asList(cache.getStats().getArtHist()));
    assertArrayEquals(new int[] {7,3,1,0,0,0,0,0,0,0},
		      cache.getStats().getArtHist());

    cache.flush();
    assertEquals(1, stats.getCacheFlushes());
    assertNull(cache.get(NS1, AUID1, URL2, -1));
    assertNull(cache.getLatest(NS1, AUID1, URL2));
    assertNull(cache.get(NS1, AUID1, URL2, 2));
    assertNull(cache.get(NS1, AUID1, URL2, 1));
  }

  @Test
  public void testFillArtCache() throws Exception {
    List<Artifact> arts = new ArrayList<>();
    for (int ii=1; ii<=10; ii++) {
      Artifact art = makeArt(NS1, AUID1, URL1, ii);
      arts.add(art);
      cache.putLatest(art);
      assertSame(art, cache.get(makeArt(NS1, AUID1, URL1, -1)));
      assertSame(art, cache.get(makeArt(NS1, AUID1, URL1, ii)));
    }
    // 10 version entries plus one latest = 11, first entry should have
    // aged out
    assertNull(cache.get(makeArt(NS1, AUID1, URL1, 1)));

    // V2-10 should still be in cache
    for (int ii=2; ii<=10; ii++) {
      assertSame(arts.get(ii-1), cache.get(makeArt(NS1, AUID1, URL1, ii)));
    }
    assertSame(arts.get(9), cache.get(makeArt(NS1, AUID1, URL1, -1)));

    cache.setMaxSize(5, 5);
    // first 6 should now be gone, leaving 7-10 and latest
    for (int ii=1; ii<7; ii++) {
      assertNull(cache.get(makeArt(NS1, AUID1, URL1, ii)));
    }
    for (int ii=7; ii<=10; ii++) {
      assertSame(arts.get(ii-1), cache.get(makeArt(NS1, AUID1, URL1, ii)));
    }
    assertSame(arts.get(9), cache.get(makeArt(NS1, AUID1, URL1, -1)));

    List<Artifact> lst = new ArrayList<>();
    for (int ii=1; ii<=10; ii++) {
      lst.add(makeArt(NS1, AUID1, URL2, ii));
    }
    for (Artifact art :
	   IteratorUtils.asIterable(cache.cachingLatestIterator(lst.iterator()))) {
    }
    ArtifactCache.Stats stats = cache.getStats();
    assertEquals(0, stats.getCacheIterHits());
    assertEquals(35, stats.getCacheHits());
    for (int ii=1; ii<=6; ii++) {
      assertNull(cache.get(NS1, AUID1, URL2, ii));
    }
    // the last 4 of these plus latest should be in cache
    for (int ii=7; ii<=10; ii++) {
      assertSame(lst.get(ii-1), cache.get(makeArt(NS1, AUID1, URL2, ii)));
    }
    assertSame(lst.get(9), cache.get(makeArt(NS1, AUID1, URL2, -1)));

    assertEquals(5, stats.getCacheIterHits());
    assertEquals(35, stats.getCacheHits());

    // and previous entries should still be there
    for (int ii=7; ii<=10; ii++) {
      assertSame(arts.get(ii-1), cache.get(makeArt(NS1, AUID1, URL1, ii)));
    }
    assertSame(arts.get(9), cache.get(makeArt(NS1, AUID1, URL1, -1)));

    assertEquals(5, stats.getCacheIterHits());
    assertEquals(40, stats.getCacheHits());
  }

  @Test
  public void testArtIterator() throws Exception {
    cache.setMaxSize(5, 5);
    ArtifactCache.Stats stats = cache.getStats();
    List<Artifact> lst = new ArrayList<>();
    for (int ii=1; ii<=10; ii++) {
      lst.add(makeArt(NS2, AUID1, URL1, ii));
    }

    Artifact s1 = makeArt(NS2, AUID1, URL2, 1);
    Artifact s2 = makeArt(NS2, AUID1, URL2, 2);
    Artifact s3 = makeArt(NS2, AUID1, URL3, 1);
    cache.putLatest(s1);
    cache.putLatest(s2);
    cache.put(lst.get(3));

    Iterator<Artifact> iter = cache.cachingLatestIterator(lst.iterator());
    assertTrue(iter.hasNext());
    Artifact a1 = iter.next();
    assertEquals(1, (int)a1.getVersion());
    assertSame(a1, lst.get(0));
    assertEquals(0, stats.getCacheHits());
    assertEquals(0, stats.getCacheIterHits());
    assertSame(a1, cache.get(NS2, AUID1, URL1, 1));
    assertEquals(0, stats.getCacheHits());
    assertEquals(1, stats.getCacheIterHits());
    assertSame(s1, cache.get(NS2, AUID1, URL2, 1));
    assertSame(s2, cache.getLatest(NS2, AUID1, URL2));
    assertEquals(2, stats.getCacheHits());
    assertEquals(1, stats.getCacheIterHits());

    Artifact a2 = iter.next();
    Artifact a3 = iter.next();
    Artifact a4 = iter.next();
    Artifact a5 = iter.next();
    assertSame(a2, lst.get(1));
    assertEquals(2, stats.getCacheHits());
    assertEquals(1, stats.getCacheIterHits());
    assertSame(a2, cache.get(NS2, AUID1, URL1, 2));
    assertSame(a3, cache.get(NS2, AUID1, URL1, 3));
    assertSame(a4, cache.get(NS2, AUID1, URL1, 4));
    assertSame(a5, cache.get(NS2, AUID1, URL1, 5));
    assertEquals(3, stats.getCacheHits());
    assertEquals(4, stats.getCacheIterHits());

    assertTrue(cache.containsIterKey(a2.makeKey()));
    cache.putLatest(s3);
    Artifact a6 = iter.next();
    assertTrue(cache.containsIterKey(a2.makeKey()));
    Artifact a7 = iter.next();
    assertFalse(cache.containsIterKey(a2.makeKey()));

    // Items in regular cache shouldn't have aged out
    assertSame(s2, cache.getLatest(NS2, AUID1, URL2));
    assertSame(s3, cache.getLatest(NS2, AUID1, URL3));
  }

  ArtifactData makeAD(String url, String id, String content) {
    return makeAD(url, id, content, null);
  }

  ArtifactData makeAD(String url, String id, String content, Map headers) {
    if (headers == null) {
      headers = MapUtil.map("H1", "v1", "h2", "V2");
    }
    ArtifactSpec s1 = ArtifactSpec.forNsAuUrl(NS1, AUID1, url)
      .setArtifactUuid(id)
      .setContent(content)
      .setHeaders(headers)
      .setCollectionDate(0);
    return s1.getArtifactData();
  }

  @Test
  public void testArtDataCache() throws Exception {
    ArtifactData ad1 = makeAD(URL1, ARTID1, "content");
    ArtifactData ad2 = makeAD(URL2, ARTID2, "content2");
    ArtifactData ad3 = makeAD(URL3, ARTID3, "content3");
    assertNull(cache.getArtifactData(NS1, ARTID1, false));
    ArtifactCache.Stats stats = cache.getStats();
    assertEquals(0, stats.getDataCacheHits());
    assertEquals(1, stats.getDataCacheMisses());

    assertSame(ad1, cache.putArtifactData(NS1, ARTID1, ad1));
    assertSame(ad1, cache.getArtifactData(NS1, ARTID1, false));
    assertEquals(1, stats.getDataCacheHits());
    assertEquals(1, stats.getDataCacheMisses());
    assertEquals(1, stats.getDataCacheStores());
    assertNull(cache.getArtifactData(NS1, ARTID2, false));
    assertEquals(1, stats.getDataCacheHits());
    assertEquals(2, stats.getDataCacheMisses());
    assertEquals(1, stats.getDataCacheStores());

    // Cached item has required InputStream
    assertSame(ad1, cache.getArtifactData(NS1, ARTID1, true));
    // Use InputStream
    ad1.getInputStream();
    // Now doesn't have required InputStream
    assertNull(cache.getArtifactData(NS1, ARTID1, true));
    // But is still valid if InputStream not required
    assertSame(ad1, cache.getArtifactData(NS1, ARTID1, false));

    assertEquals(3, stats.getDataCacheHits());
    assertEquals(3, stats.getDataCacheMisses());

    assertSame(ad2, cache.putArtifactData(NS1, ARTID2, ad2));
    assertSame(ad3, cache.putArtifactData(NS1, ARTID3, ad3));

    assertArrayEquals(new int[] {4,0,0,0,0,0,0,0,0,0},
		      cache.getStats().getArtDataHist());
    // This is now 3rd
    assertSame(ad1, cache.getArtifactData(NS1, ARTID1, false));
    // now 2nd
    assertSame(ad3, cache.getArtifactData(NS1, ARTID3, false));
    // 3rd
    assertSame(ad2, cache.getArtifactData(NS1, ARTID2, false));
    log.trace("artdatahist: {}",
	      Arrays.asList(cache.getStats().getArtDataHist()));
    assertArrayEquals(new int[] {4,1,2,0,0,0,0,0,0,0},
		      cache.getStats().getArtDataHist());

    // Ensure unused item in cache isn't replaced by used new item
    ArtifactData ad2a = makeAD(URL2, ARTID2, "content2");
    ArtifactData ad2b = makeAD(URL2, ARTID2, "content2");
    ad2a.getInputStream();
    // ad2a is used, shouldn't replace unused ad2
    assertSame(ad2, cache.putArtifactData(NS1, ARTID2, ad2a));
    assertSame(ad2, cache.getArtifactData(NS1, ARTID2, true));
    // ad2b not used, should replace ad2
    assertSame(ad2b, cache.putArtifactData(NS1, ARTID2, ad2b));
    assertSame(ad2b, cache.getArtifactData(NS1, ARTID2, true));
    ad2b.getInputStream();
    // now ad2a should replace used ad2b
    assertSame(ad2a, cache.putArtifactData(NS1, ARTID2, ad2a));
    assertSame(ad2a, cache.getArtifactData(NS1, ARTID2, false));
  }

}
