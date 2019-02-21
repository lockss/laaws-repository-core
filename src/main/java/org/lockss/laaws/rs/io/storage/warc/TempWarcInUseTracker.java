/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.laaws.rs.io.storage.warc;

import java.util.HashMap;
import java.util.Map;

/**
 * Keeps track of which temporary WARC files are currently being accessed.
 */
public enum TempWarcInUseTracker {
  INSTANCE;

  // The files that are in use.
  private final Map<String, Integer> inUseMap = new HashMap<>();

  /**
   * Records that an individual use of a temporary WARC file has started.
   * 
   * @param path
   *          A String with the storage URL path to the temporary WARC file.
   */
  public synchronized void markUseStart(String path) {
    inUseMap.putIfAbsent(path, 0);
    inUseMap.put(path, inUseMap.get(path) + 1);
  }

  /**
   * Records that an individual use of a temporary WARC file has ended.
   * 
   * @param path
   *          A String with the storage URL path to the temporary WARC file.
   */
  public synchronized void markUseEnd(String path) {
    Integer count = inUseMap.get(path);

    if (count == null) {
      throw new IllegalStateException(
	  "Attempt to decrement past zero for WARC file '" + path + "'");
    } else if (count.intValue() == 1) {
      inUseMap.remove(path);
    } else {
      inUseMap.put(path, count.intValue() - 1);
    }
  }

  /**
   * Provides an indication of whether a temporary WARC file is in use.
   * 
   * @param path
   *          A String with the storage URL path to the temporary WARC file.
   * @return <code>true</code> if the temporary WARC file is in use,
   *         <code>false</code> otherwise.
   */
  public synchronized boolean isInUse(String path) {
    return inUseMap.get(path) != null;
  }

  @Override
  public synchronized String toString() {
    return "[TempWarcInUseTracker inUseMap=" + inUseMap + "]";
  }
}
