/*

 Copyright (c) 2019-2020 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.model;

import org.junit.jupiter.api.*;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.os.*;

public class TestStorageInfo extends LockssTestCase5 {
  private StorageInfo identifier;

  @Test
  public void testFromDF() throws Exception {
    String tmpdir = getTempDir().toString();
    PlatformUtil.DF df = PlatformUtil.getInstance().getDF(tmpdir);
    StorageInfo si = StorageInfo.fromDF(df);
    assertEquals(df.getSize(), si.getSize() / 1024); // From DF in KB.
    assertEquals("disk", si.getType());
    assertEquals(df.getMnt(), si.getName());
    assertEquals(df.getUsed(), si.getUsed() / 1024); // From DF in KB.
    assertEquals(df.getAvail(), si.getAvail() / 1024); // From DF in KB.
    assertEquals(df.getPercentString(), si.getPercentUsedString());

    StorageInfo si2 = StorageInfo.fromDF("notdisk", df);
    assertFalse(si.isSameDevice(si2));
  }
}
