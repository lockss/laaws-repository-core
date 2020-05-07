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

package org.lockss.laaws.rs.io.storage.local;

import org.apache.commons.io.FileUtils;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.WarcFilePool;
import org.lockss.log.L4JLogger;
import org.lockss.util.PatternIntMap;
import org.lockss.util.io.FileUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.storage.StorageInfo;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Version of LocalWarcArtifactDataStore that allows manipulating apparent
 * free space
 */
public class TestingWarcArtifactDataStore extends LocalWarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();
  private PatternIntMap freeSpacePatternMap;

  public TestingWarcArtifactDataStore(ArtifactIndex index, File basePath) throws IOException {
    super(index, basePath);
  }

  public TestingWarcArtifactDataStore(ArtifactIndex index, File[] basePath)
      throws IOException {
    super(index, basePath);
  }

  public TestingWarcArtifactDataStore(ArtifactIndex index, Path basePaths)
      throws IOException {
    super(index, basePaths);
  }

  public TestingWarcArtifactDataStore(ArtifactIndex index, Path[] basePaths)
      throws IOException {
    super(index, basePaths);
  }

  @Override
  protected long getFreeSpace(Path fsPath) {
    if (freeSpacePatternMap != null) {
      long fake = freeSpacePatternMap.getMatch(fsPath.toString(), -1);
      if (fake > 0) {
	log.debug("Returning fake free space for {}: {}", fsPath, fake);
	return fake;
      }
    }
    return super.getFreeSpace(fsPath);
  }

  public void setTestingDiskSpaceMap(PatternIntMap freeSpacePatternMap) {
    this.freeSpacePatternMap = freeSpacePatternMap;
    log.debug2("freeSpacePatternMap: {}", freeSpacePatternMap);
  }

}
