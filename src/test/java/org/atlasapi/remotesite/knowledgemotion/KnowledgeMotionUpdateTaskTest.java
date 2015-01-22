package org.atlasapi.remotesite.knowledgemotion;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.googlespreadsheet.SpreadsheetFetcher;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.metabroadcast.common.scheduling.ScheduledTask;

@RunWith(MockitoJUnitRunner.class)
public class KnowledgeMotionUpdateTaskTest {

    private final SpreadsheetFetcher spreadsheetFetcher = mock(SpreadsheetFetcher.class);
    private final KnowledgeMotionAdapter adapter = mock(KnowledgeMotionAdapter.class);
    private final DefaultKnowledgeMotionDataRowHandler dataHandler = mock(DefaultKnowledgeMotionDataRowHandler.class);
    private final ContentLister contentLister = mock(ContentLister.class);
    private final ScheduledTask task = new KnowledgeMotionUpdateTask(KnowledgeMotionModule.SOURCES, spreadsheetFetcher, dataHandler, adapter, contentLister);
    
    @Test
    public void testTask() {
        SpreadsheetEntry spreadsheet = new SpreadsheetEntry();
        WorksheetEntry worksheet = new WorksheetEntry();
        ListFeed feed = new ListFeed();
        ListEntry entry = new ListEntry();
        entry.getCustomElements().setValueLocal(KnowledgeMotionSpreadsheetColumn.SOURCE.getValue(), "Arbitrary Source");
        feed.setEntries(ImmutableList.of(entry));
        
        when(spreadsheetFetcher.getSpreadsheetByTitle(Matchers.anyString())).thenReturn(ImmutableList.of(spreadsheet));
        when(spreadsheetFetcher.getWorksheetsFromSpreadsheet(spreadsheet)).thenReturn(ImmutableList.of(worksheet));
        when(spreadsheetFetcher.getDataFromWorksheet(worksheet)).thenReturn(feed);

        Content c1 = mock(Content.class); when(c1.getCanonicalUri()).thenReturn("blah");
        Content c2 = mock(Content.class); when(c2.getCanonicalUri()).thenReturn("blahh");
        Content c3 = mock(Content.class); when(c3.getCanonicalUri()).thenReturn("blahhh");
        when(dataHandler.handle(Matchers.any(KnowledgeMotionDataRow.class))).thenReturn(Optional.of(c1));
        
        when(contentLister.listContent(Matchers.any(ContentListingCriteria.class))).thenReturn(ImmutableList.of(c1,c2,c3).iterator());
        
        task.run();
        verify(adapter, times(1)).dataRow(Iterables.getOnlyElement(feed.getEntries()).getCustomElements());
        verify(dataHandler, times(1)).handle(Matchers.any(KnowledgeMotionDataRow.class));
        verify(dataHandler, times(2)).write(Matchers.any(Content.class));  // we expect the unseen content to be marked unavailable
    }
    
}
