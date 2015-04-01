package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

public enum KnowledgeMotionSpreadsheetColumn {

    /*
     * Knowledgemotion's CSVs contain two 'Source' columns. This is a problem.
     * However, one of those columns is always the first column. Therefore,
     * just get that field by index 0.
     */
    SOURCE("Source"),
    ID("namespace"),
    TITLE("Title"),
    DESCRIPTION("Description"),
    DATE("Date"),
    DURATION("Duration"),
    KEYWORDS("Keywords"),
    PRICE_CATEGORY("Price category (1= stock, 2=news, 3=brand)"),
    ALT_ID("Alternative ID"),
    TERMS_OF_USE("Terms of Use")
    ;
    
    private final String fieldName;
    
    private KnowledgeMotionSpreadsheetColumn(String fieldName) {
        this.fieldName = checkNotNull(fieldName);
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
}
