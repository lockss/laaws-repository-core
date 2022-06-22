package org.lockss.laaws.rs.model;

public interface AuJournalEntry {
    String LOCKSS_MD_ARTIFACTID_KEY = "artifactId";
    String JOURNAL_ENTRY_DATE= "entryDate";

    String getArtifactId();
    long getEntryDate();
}
