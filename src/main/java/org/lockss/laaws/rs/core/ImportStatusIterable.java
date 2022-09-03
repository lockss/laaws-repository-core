package org.lockss.laaws.rs.core;

import org.lockss.laaws.rs.model.ImportStatus;

import java.io.InputStream;

public class ImportStatusIterable extends JsonSequenceIterable<ImportStatus> {
  public ImportStatusIterable(InputStream input) {
    super(input, ImportStatus.class);
  }
}
