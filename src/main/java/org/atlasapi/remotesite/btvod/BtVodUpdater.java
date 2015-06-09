package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class BtVodUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BtVodUpdater.class);
    
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final String uriPrefix;
    private final Publisher publisher;
    private final BtVodData vodData;
    private final BtVodContentGroupUpdater contentGroupUpdater;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    
    public BtVodUpdater(ContentResolver resolver, ContentWriter contentWriter,
            BtVodData vodData, String uriPrefix, 
            BtVodContentGroupUpdater contentGroupUpdater, 
            BtVodDescribedFieldsExtractor describedFieldsExtractor, 
            Publisher publisher) {
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(contentWriter);
        this.vodData = checkNotNull(vodData);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.publisher = checkNotNull(publisher);
        this.contentGroupUpdater = checkNotNull(contentGroupUpdater);
        
        withName("BT VOD Catalogue Ingest");
    }
    
    @Override
    public void runTask() {
        contentGroupUpdater.start();
        Set<String> processedRows = Sets.newHashSet();
        BtVodBrandWriter brandExtractor = new BtVodBrandWriter(
                writer,
                resolver,
                publisher,
                uriPrefix,
                contentGroupUpdater,
                processedRows,
                new TitleSanitiser(), 
                describedFieldsExtractor
        );
        BtVodSeriesWriter seriesExtractor = new BtVodSeriesWriter(
                writer,
                resolver,
                brandExtractor,
                publisher,
                contentGroupUpdater,
                describedFieldsExtractor, 
                processedRows
        );
        BtVodItemWriter itemExtractor = new BtVodItemWriter(
                writer,
                resolver,
                brandExtractor,
                seriesExtractor,
                publisher,
                uriPrefix,
                contentGroupUpdater,
                describedFieldsExtractor,
                processedRows,
                new TitleSanitiser()
        );
        try {
            reportStatus("Brand extract [IN PROGRESS]  Series extract [TODO]  Item extract [TODO]");

            vodData.processData(brandExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [IN PROGRESS]  Item extract [TODO]  Content group update [TODO]", 
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures()));

            vodData.processData(seriesExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [DONE: %d rows successful %d rows failed]  Item extract [IN PROGRESS]  Content group update [TODO]", 
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures(), 
                    seriesExtractor.getResult().getProcessed(), 
                    seriesExtractor.getResult().getFailures()));

            vodData.processData(itemExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [DONE: %d rows successful %d rows failed]  Item extract [DONE: %d rows successful %d rows failed] Content group update [IN PROGRESS]",
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures(), 
                    seriesExtractor.getResult().getProcessed(), 
                    seriesExtractor.getResult().getFailures(),
                    itemExtractor.getResult().getProcessed(),
                    itemExtractor.getResult().getFailures()));
            
            contentGroupUpdater.finish();
            
            if (brandExtractor.getResult().getFailures() > 0
                    || seriesExtractor.getResult().getFailures() > 0
                    || itemExtractor.getResult().getFailures() > 0) {
                throw new RuntimeException("Failed to extract some rows");
            }
        } catch (IOException e) {
            log.error("Extraction failed", e);
            Throwables.propagate(e);
        }
        
    }
    
    
}
