package org.atlasapi.remotesite.knowledgemotion;

import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DATE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DESCRIPTION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DURATION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.ID;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.KEYWORDS;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.SOURCE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.*;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.gdata.data.spreadsheet.CustomElementCollection;

// TODO move these imported column definitions to constants in the single file that actually needs to use them (KnowledgeMotionDataRow)

public class KnowledgeMotionAdapter {

private final Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
    
    public KnowledgeMotionDataRow dataRow(CustomElementCollection customElements) {
        KnowledgeMotionDataRow.Builder row = KnowledgeMotionDataRow.builder();
        
        row.withDate(customElements.getValue(DATE.getValue()));
        row.withDescription(customElements.getValue(DESCRIPTION.getValue()));
        row.withDuration(customElements.getValue(DURATION.getValue()));
        row.withId(customElements.getValue(ID.getValue()));
        row.withKeywords(keywords(customElements.getValue(KEYWORDS.getValue())));
        row.withSource(customElements.getValue(SOURCE.getValue()));
        row.withTitle(customElements.getValue(TITLE.getValue()));
        row.withAlternativeId(customElements.getValue(ALT_ID.getValue()));
        
        return row.build();
    }

    private Iterable<String> keywords(String allKeywords) {
        return allKeywords == null ? ImmutableList.<String>of() : splitter.split(allKeywords);
    }
    
}
