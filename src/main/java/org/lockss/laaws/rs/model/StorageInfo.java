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

import java.io.Serializable;
import org.lockss.util.os.PlatformUtil;

/**
 * Information about a storage area, such as used and free space
 */
public class StorageInfo implements Serializable {

  private String type;   // Currently just informative: "disk", "memory", etc.
  private String name;   // Indentifying name such as mount point
  private long size = -1; // Size in bytes of the storage area.
  private long used = -1; // Size in bytes of the used storage area.
  private long avail = -1; // Size in bytes of the available storage area
  private String percentUsedString;
  private double percentUsed = -1.0;

  /**
   * Default constructor.
   */
  public StorageInfo() {
  }

  /** Create a StorageInfo representing the disk usage information in the
   * DF structure
   * @param df disk usage info from PlatformUtil
   * @return StorageInfo
   */
  public static StorageInfo fromDF(PlatformUtil.DF df) {
    return fromDF("disk", df);
  }

  /** Create a StorageInfo representing the disk usage information in the
   * DF structure
   * @param type type string
   * @param df disk usage info from PlatformUtil
   * @return StorageInfo
   */
  public static StorageInfo fromDF(String type, PlatformUtil.DF df) {
    StorageInfo res = new StorageInfo(type);
    if (df != null) {
      res.name = df.getMnt();
      res.size = df.getSize() * 1024; // From DF in KB, here in bytes.
      res.used = df.getUsed() * 1024; // From DF in KB, here in bytes.
      res.avail = df.getAvail() * 1024; // From DF in KB, here in bytes.
      res.percentUsedString = df.getPercentString();
      res.percentUsed = df.getPercent();
    }
    return res;
  }

  /** Create a StorageInfo containing only a type string
   * @param type type string
   * @return StorageInfo
   */
  public StorageInfo(String type) {
    this.type = type;
  }

  /** Return storage type: {@code disk}, {@code memory}, etc. */
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  /** Return storage name, e.g., mount point */
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /** Return total size in bytes */
  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  /** Return used size in bytes */
  public long getUsed() {
    return used;
  }

  public void setUsed(long used) {
    this.used = used;
  }

  /** Return available size in bytes */
  public long getAvail() {
    return avail;
  }

  public void setAvail(long avail) {
    this.avail = avail;
  }

  /** Return percent used as a string: <code><i>nn<i>%</code> */
  public String getPercentUsedString() {
    return percentUsedString;
  }

  public void setPercentUsedString(String percentUsedString) {
    this.percentUsedString = percentUsedString;
  }

  /** Return percent used as a double between 0.0 and 1.0 */
  public double getPercentUsed() {
    return percentUsed;
  }

  public void setPercentUsed(double percentUsed) {
    this.percentUsed = percentUsed;
  }

  /** Return true if on the same device as <i>other<i>.  I.e., if the name
   * and type are the same */
  public boolean isSameDevice(StorageInfo other) {
    if (other == null || name == null || type == null) {
      return false;
    }
    return type.equals(other.getType()) && name.equals(other.getName());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[StorageInfo");
    addIf(sb, "type", type);
    addIf(sb, "name", name);
    addIf(sb, "size", size);
    addIf(sb, "used", used);
    addIf(sb, "avail", avail);
    addIf(sb, "used %", percentUsedString);
    sb.append("]");
    return sb.toString();
  }

  private static void addIf(StringBuilder sb, String label, long val) {
    if (val >= 0) {
      sb.append(" ");
      sb.append(label);
      sb.append(": ");
      sb.append(val);
    }
  }

  private static void addIf(StringBuilder sb, String label, String val) {
    if (val != null) {
      sb.append(" ");
      sb.append(label);
      sb.append(": ");
      sb.append(val);
    }
  }
}
