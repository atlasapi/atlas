package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

public enum KnowledgeMotionSpreadsheetColumn {

    SOURCE("Source"),
    ID("namespace"),
    TITLE("Title"),
    DESCRIPTION("Description"),
    DATE("Date"),
    DURATION("Duration"),
    KEYWORDS("Keywords"),
    ALT_ID("AlternativeID")  // lack of space believed to be intentional (quirk of indexing into returned rows)
    ;
    
    private final String fieldName;
    
    private KnowledgeMotionSpreadsheetColumn(String fieldName) {
        this.fieldName = checkNotNull(fieldName);
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
}
