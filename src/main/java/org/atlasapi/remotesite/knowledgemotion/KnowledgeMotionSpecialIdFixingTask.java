package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Set;

import org.atlasapi.googlespreadsheet.SpreadsheetFetcher;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gdata.data.spreadsheet.CustomElementCollection;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class KnowledgeMotionSpecialIdFixingTask extends ScheduledTask {
    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionSpecialIdFixingTask.class);

    private static final String JOB_KEY = "km-spreadsheet-id-fixer";

    private final String spreadsheetTitle = Configurer.get("google.spreadsheet.title").get();
    private final SpreadsheetFetcher spreadsheetFetcher;
    private final KnowledgeMotionAdapter adapter;
    private final KnowledgeMotionDataRowHandler dataHandler;

    private final ImmutableList<Publisher> allKmPublishers;
    private final ContentLister contentLister;

    public KnowledgeMotionSpecialIdFixingTask(Iterable<KnowledgeMotionSourceConfig> sources,
            SpreadsheetFetcher spreadsheetFetcher,
            SpecialIdFixingKnowledgeMotionDataRowHandler dataHandler, KnowledgeMotionAdapter adapter,
            ContentLister contentLister) {
        this.spreadsheetFetcher = checkNotNull(spreadsheetFetcher);
        this.dataHandler = checkNotNull(dataHandler);
        this.adapter = checkNotNull(adapter);

        this.allKmPublishers = ImmutableList.copyOf(Iterables.transform(sources, new Function<KnowledgeMotionSourceConfig, Publisher>(){
            @Override public Publisher apply(KnowledgeMotionSourceConfig input) {
                return input.publisher();
            }}));
        this.contentLister = checkNotNull(contentLister);
    }

    @Override
    protected void runTask() {
        try {
            ListFeed data = fetchData();
            KnowledgeMotionDataProcessor<UpdateProgress> processor = processor();
            for (ListEntry row : data.getEntries()) {
                processor.process(row.getCustomElements());
            }
            reportStatus(processor.getResult().toString() + " – finished.");
        } catch (Exception e) {
            reportStatus(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    private ListFeed fetchData() {
        SpreadsheetEntry spreadsheet = Iterables.getOnlyElement(spreadsheetFetcher.getSpreadsheetByTitle(getModifiedTitle()));
        WorksheetEntry worksheet = Iterables.getOnlyElement(spreadsheetFetcher.getWorksheetsFromSpreadsheet(spreadsheet));
        return spreadsheetFetcher.getDataFromWorksheet(worksheet);
    }

    //Replace spaces with dashes (something weird happening in jetty)
    private String getModifiedTitle() {
        return spreadsheetTitle.replace("-", " ");
    }

    private KnowledgeMotionDataProcessor<UpdateProgress> processor() {
        return new KnowledgeMotionDataProcessor<UpdateProgress>() {

            UpdateProgress progress = UpdateProgress.START;

//            Set<String> seenUris = Sets.newHashSet();

            @Override
            public boolean process(CustomElementCollection customElements) {
                try {
                    KnowledgeMotionDataRow row = adapter.dataRow(customElements);
                    Optional<Content> written = dataHandler.handle(row);
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.warn("Row: " + customElements.getValue(KnowledgeMotionSpreadsheetColumn.ID.getValue()), e);
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
                reportStatus(progress.toString());
                return shouldContinue();
            }

            public ImmutableSet<String> seenUris() {
                return null;  // TODO yayyy (get rid of this interface)
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }

}
