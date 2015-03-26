package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionUpdater {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionUpdater.class);

    private final KnowledgeMotionDataRowHandler dataHandler;
    private final Iterable<KnowledgeMotionSourceConfig> allKmPublishers;
    private final ContentLister contentLister;

    private Set<String> seenUris;

    public KnowledgeMotionUpdater(Iterable<KnowledgeMotionSourceConfig> sources,
        KnowledgeMotionContentMerger dataHandler,
        ContentLister contentLister) {
        this.dataHandler = checkNotNull(dataHandler);
        this.allKmPublishers = checkNotNull(sources);
        this.contentLister = checkNotNull(contentLister);

        seenUris = Sets.newHashSet();
    }

    protected ProcessingResult process(List<KnowledgeMotionDataRow> rows, ProcessingResult processingResult) {
        boolean allRowsSuccess = true;

        String publisherRowHeader = rows.get(0).getSource();
        Publisher publisher = null;
        for (KnowledgeMotionSourceConfig config : allKmPublishers) {
            if (publisherRowHeader.equals(config.rowHeader())) {
                publisher = config.publisher();
            }
        }
        if (publisher == null) {
            StringBuilder errorText = new StringBuilder();
            errorText.append("First row did not contain a recognised publisher in the 'Source' column.").append("\n");
            errorText.append("Found: " + publisherRowHeader).append("\n");
            errorText.append("Valid publishers are: ").append("\n");
            for (KnowledgeMotionSourceConfig config : allKmPublishers) {
                errorText.append(config.rowHeader()).append("\n");
            }

            processingResult.error("input file", errorText.toString());
        }

        for (KnowledgeMotionDataRow row : rows) {
            try {
                Optional<Content> written = dataHandler.handle(row);
                if (written.isPresent()) {
                    seenUris.add(written.get().getCanonicalUri());
                }
                processingResult.success();
            } catch (RuntimeException e) {
                allRowsSuccess = false;
                log.info("Failed to update", e);
                processingResult.error(row.getId(), "While merging content: " + e.getMessage());
            }
        }

        /*
         * If all rows of this processing run completed successfully,
         * unpublish everything else by this publisher
         */
        if (allRowsSuccess && rows.size() > 0) {
            Iterator<Content> allStoredKmContent = contentLister.listContent(ContentListingCriteria.defaultCriteria().forContent(ContentCategory.TOP_LEVEL_ITEM).forPublisher(publisher).build());
            while (allStoredKmContent.hasNext()) {
                Content item = allStoredKmContent.next();
                if (!seenUris.contains(item.getCanonicalUri())) {
                    item.setActivelyPublished(false);
                    dataHandler.write(item);
                }
            }
        }

        return processingResult;
    }

}
