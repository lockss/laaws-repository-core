/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.log.L4JLogger;
import org.springframework.http.HttpHeaders;

/** Utilities for V2 repository
 */
public class RepoUtil {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Build a Spring HttpHeaders from CIProperties */
  public static HttpHeaders httpHeadersFromMap(Map map) {
    HttpHeaders res = new HttpHeaders();
    for (String key : (Set<String>) (map.keySet())) {
      res.set(key, (String)map.get(key));
    }
    return res;
  }

  /** Build a Map from a Spring HttpHeaders */
  // TK should concatenate multi-value keys
  public static Map<String,String> mapFromHttpHeaders(HttpHeaders hdrs) {
    Map<String,String> res = new HashMap<String,String>();
    for (String key : hdrs.keySet()) {
      res.put(key, String.join(",", hdrs.get(key)));
    }
    return res;
  }

//   public static Artifact storeArt(LockssRepository repo, String coll,
// 				  String auid, String url, InputStream in,
// 				  CIProperties props) throws IOException {
//     ArtifactIdentifier id = new ArtifactIdentifier(coll, auid,
// 						   url, null);
//     CIProperties propsCopy  = CIProperties.fromProperties(props);
//     propsCopy.setProperty(CachedUrl.PROPERTY_NODE_URL, url);
//     HttpHeaders metadata = httpHeadersFromProps(propsCopy);

//     // tk
//     BasicStatusLine statusLine =
//       new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

//     ArtifactData ad =
//       new ArtifactData(id, metadata,
// 		       new IgnoreCloseInputStream(in),
// 		       statusLine);
//     if (log.isDebug2()) {
//       log.debug2("Creating artifact: " + ad);
//     }
//     Artifact uncommittedArt = repo.addArtifact(ad);
//     return repo.commitArtifact(uncommittedArt);
//   }


}
