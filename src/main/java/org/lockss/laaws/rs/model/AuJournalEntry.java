package org.lockss.laaws.rs.model;

import java.time.Instant;

public interface AuJournalEntry {
    String LOCKSS_MD_ARTIFACTID_KEY = "artifactId";
    String JOURNAL_ENTRY_DATE= "entryDate";

    String getArtifactId();
    Instant getEntryDate();
}
