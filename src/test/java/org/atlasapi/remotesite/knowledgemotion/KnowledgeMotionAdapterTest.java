package org.atlasapi.remotesite.knowledgemotion;

import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DATE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DESCRIPTION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DURATION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.ID;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.KEYWORDS;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.SOURCE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.TITLE;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.gdata.data.spreadsheet.CustomElementCollection;

public class KnowledgeMotionAdapterTest {

private static final KnowledgeMotionAdapter adapter = new KnowledgeMotionAdapter();
    
    private CustomElementCollection customElements() {
        CustomElementCollection customElements = new CustomElementCollection();
        customElements.setValueLocal(DATE.getFieldName(), "date");
        customElements.setValueLocal(DESCRIPTION.getFieldName(), "description");
        customElements.setValueLocal(DURATION.getFieldName(), "duration");
        customElements.setValueLocal(ID.getFieldName(), "id");
        customElements.setValueLocal(KEYWORDS.getFieldName(), "keywords");
        customElements.setValueLocal(SOURCE.getFieldName(), "source");
        customElements.setValueLocal(TITLE.getFieldName(), "title");
        return customElements;
    }

    @Test
    public void testRowConversionFromCustomElements() {
        KnowledgeMotionDataRow globalImageDataRow = adapter.dataRow(customElements());
        assertEquals("date", globalImageDataRow.getDate());
        assertEquals("description", globalImageDataRow.getDescription());
        assertEquals("duration", globalImageDataRow.getDuration());
        assertEquals("id", globalImageDataRow.getId());
        assertEquals("source", globalImageDataRow.getSource());
        assertEquals("title", globalImageDataRow.getTitle());
        assertEquals(ImmutableList.of("keywords"), globalImageDataRow.getKeywords());
    }
    
}
