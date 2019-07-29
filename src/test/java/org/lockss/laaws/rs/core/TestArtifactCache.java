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
import org.lockss.laaws.rs.model.Artifact;
import org.junit.jupiter.api.*;
import org.lockss.util.test.LockssTestCase5;

public class TestArtifactCache extends LockssTestCase5 {

  static String COLL1 = "c1";
  static String COLL2 = "c2";
  static String AUID1 = "AAUU1";
  static String AUID2 = "AAUU2";

  static String URL1 = "u1";
  static String URL2 = "u2";
  static String URL3 = "u3";

  private ArtifactCache cache;

  @BeforeEach
  public void setUp() throws Exception {
    cache = new ArtifactCache(10);
  }

  Artifact makeArt(String coll, String auid, String url, int version) {
    return new Artifact("aidid", coll, auid, url, version, Boolean.TRUE,
			"store_url", 123, null);
  }

  @Test
  public void testCaching() throws Exception {
    Artifact u1v1 = makeArt(COLL1, AUID1, URL1, 1);
    Artifact u2v1 = makeArt(COLL1, AUID1, URL2, 1);
    Artifact u2v2 = makeArt(COLL1, AUID1, URL2, 2);
    Artifact u3v1 = makeArt(COLL1, AUID1, URL3, 1);
    assertNull(cache.get(u1v1));
    assertEquals(0, cache.getCacheHits());
    assertEquals(1, cache.getCacheMisses());

    assertSame(u1v1, cache.put(u1v1));
    assertSame(u1v1, cache.get(u1v1));
    assertSame(u1v1, cache.get(makeArt(COLL1, AUID1, URL1, 1)));

    assertEquals(2, cache.getCacheHits());
    assertEquals(1, cache.getCacheMisses());

    assertNull(cache.get(makeArt(COLL1, AUID1, URL1, -1)));
    assertSame(u2v1, cache.putLatest(u2v1));
    assertSame(u2v1, cache.get(u2v1));
    assertSame(u2v1, cache.get(makeArt(COLL1, AUID1, URL2, 1)));
    assertSame(u2v1, cache.get(COLL1, AUID1, URL2, -1));
    assertEquals(-1, (int)makeArt(COLL1, AUID1, URL2, -1).getVersion());
    assertSame(u2v1, cache.get(makeArt(COLL1, AUID1, URL2, -1)));
    assertEquals(6, cache.getCacheHits());
    assertEquals(2, cache.getCacheMisses());

    assertSame(u2v2, cache.putLatest(u2v2));
    assertSame(u2v2, cache.get(COLL1, AUID1, URL2, -1));
    assertSame(u2v2, cache.get(COLL1, AUID1, URL2, 2));
    assertSame(u2v1, cache.get(COLL1, AUID1, URL2, 1));

  }

  @Test
  public void testFillCache() throws Exception {
    List<Artifact> arts = new ArrayList<>();
    for (int ii=1; ii<11; ii++) {
      Artifact art = makeArt(COLL1, AUID1, URL1, ii);
      arts.add(art);
      cache.putLatest(art);
      assertSame(art, cache.get(makeArt(COLL1, AUID1, URL1, -1)));
      assertSame(art, cache.get(makeArt(COLL1, AUID1, URL1, ii)));
    }
    // 10 version entries plus one latest = 11, first entry should have
    // aged out
    assertNull(cache.get(makeArt(COLL1, AUID1, URL1, 1)));

    // V2-10 should still be in cache
    for (int ii=2; ii<11; ii++) {
      assertSame(arts.get(ii-1), cache.get(makeArt(COLL1, AUID1, URL1, ii)));
    }
    assertSame(arts.get(9), cache.get(makeArt(COLL1, AUID1, URL1, -1)));
  }

}
