/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.io.storage.warc;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Write real tests
 */
class TestWarcFilePool {
  private final static L4JLogger log = L4JLogger.getLogger();

  class RunnableDemo implements Runnable {
    private final long baseWait;
    private WarcFilePool warcFilePool;
    private Thread t;
    private String threadName;

    public RunnableDemo(String name, WarcFilePool warcFilePool, long baseWait) {
      this.threadName = name;
      this.warcFilePool = warcFilePool;
      this.baseWait = baseWait;
    }

    @Override
    public void run() {

      try {
        for (int i = 0; i < 1000; i++) {
          long bytesExpected = (long) (Math.random() * FileUtils.ONE_MB * 20.1f);
          WarcFile warcFile = warcFilePool.getWarcFile(bytesExpected);
//          log.info(String.format("%s: Got: %s: Length: %d", threadName, warcFile.getPath(), warcFile.getLength()));

          warcFile.setLength(warcFile.getLength() + bytesExpected);
//          log.info(String.format("%s: Wrote: %d bytes to %s", threadName, bytesExpected, warcFile.getPath()));

          if (false) {
            Thread.sleep((long) (Math.random() * 100f));
          }

          if (warcFile != null) {
            if (Math.random() * 100 < 606) {
              warcFilePool.returnWarcFile(warcFile);
            } else {
              log.info(String.format("%s: Not returning: %s", threadName, warcFile.getPath()));
            }
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }

    public Thread start() throws InterruptedException {
      if (t == null) {
        t = new Thread(this, threadName);
        t.start();
      }

      return t;
    }
  }

  @Test
  void findWarcFile() throws Exception {
    for (int i= 0 ; i< 10; i++) {
      WarcFilePool pool = new WarcFilePool("/tmp");
      List<Thread> threads = new ArrayList<>();

      for (int j = 0; j < 10; j++) {
        RunnableDemo rd = new RunnableDemo("t"+String.valueOf(j), pool, 0);
        Thread.sleep((long) (Math.random() * 250));
        threads.add(rd.start());
      }

      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });

      log.info(String.format("Run %d:", i+1));
      pool.dumpWarcFilesPoolInfo();
    }
  }
}