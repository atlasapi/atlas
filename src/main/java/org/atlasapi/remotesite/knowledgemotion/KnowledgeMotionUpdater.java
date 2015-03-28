package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
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

    protected ProcessingResult process(Iterator<KnowledgeMotionDataRow> rows, ProcessingResult processingResult) {
        if (!rows.hasNext()) {
            log.info("Knowledgemotion Common Ingest received an empty file");
            processingResult.error("input file", "Empty file");
        }

        KnowledgeMotionDataRow firstRow = rows.next();
        String publisherRowHeader = firstRow.getSource();
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

        boolean allRowsSuccess = true;
        if (!writeRow(firstRow, processingResult)) {
            allRowsSuccess = false;
        }
        while (rows.hasNext()) {
            if (!writeRow(rows.next(), processingResult)) {
                allRowsSuccess = false;
            }
        }

        /*
         * If all rows of this processing run completed successfully,
         * unpublish everything else by this publisher
         */
        if (allRowsSuccess) {
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

    /**
     * @return success
     */
    private boolean writeRow(KnowledgeMotionDataRow row, ProcessingResult processingResult) {
        try {
            Optional<Content> written = dataHandler.handle(row);
            if (written.isPresent()) {
                seenUris.add(written.get().getCanonicalUri());
            }
            log.debug("Successfully updated row {}", row.getId());
            processingResult.success();
            return true;
        } catch (RuntimeException e) {
            log.debug("Failed to update row {}", row.getId(), e);
            processingResult.error(row.getId(), "While merging content: " + e.getMessage());
            return false;
        }
    }

}
