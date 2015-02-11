package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

public enum KnowledgeMotionSpreadsheetColumn {

    SOURCE("Source"),
    ID("namespace"),
    TITLE("Title"),
    DESCRIPTION("Description"),
    DATE("Date"),
    DURATION("Duration"),
    KEYWORDS("Keywords")
    ;
    
    private final String fieldName;
    
    private KnowledgeMotionSpreadsheetColumn(String fieldName) {
        this.fieldName = checkNotNull(fieldName);
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
}
