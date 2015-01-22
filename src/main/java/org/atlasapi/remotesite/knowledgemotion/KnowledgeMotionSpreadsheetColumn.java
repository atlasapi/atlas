package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

public enum KnowledgeMotionSpreadsheetColumn {

    SOURCE("source"),
    ID("namespace"),
    TITLE("Title"),
    DESCRIPTION("Description"),
    DATE("Date"),
    DURATION("Duration"),
    KEYWORDS("Keywords"),
    ALT_ID("AlternativeID")  // lack of space believed to be intentional (quirk of indexing into returned rows)
    ;
    
    private final String value;
    
    private KnowledgeMotionSpreadsheetColumn(String value) {
        this.value = checkNotNull(value);
    }
    
    public String getValue() {
        return value;
    }
    
}
