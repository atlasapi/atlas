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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

public class KnowledgeMotionUpdater {

    private final KnowledgeMotionDataRowHandler dataHandler;
    private final ImmutableList<Publisher> allKmPublishers;
    private final ContentLister contentLister;

    private Set<String> seenUris;

    public KnowledgeMotionUpdater(Iterable<KnowledgeMotionSourceConfig> sources,
        KnowledgeMotionContentMerger dataHandler,
        ContentLister contentLister) {
        this.dataHandler = checkNotNull(dataHandler);
        this.allKmPublishers = ImmutableList.copyOf(Iterables.transform(sources, new Function<KnowledgeMotionSourceConfig, Publisher>(){
            @Override public Publisher apply(KnowledgeMotionSourceConfig input) {
                return input.publisher();
            }}));
        this.contentLister = checkNotNull(contentLister);

        seenUris = Sets.newHashSet();
    }

    protected ProcessingResult process(List<KnowledgeMotionDataRow> rows, ProcessingResult processingResult) {
        boolean allRowsSuccess = true;

        for (KnowledgeMotionDataRow row : rows) {
            try {
                Optional<Content> written = dataHandler.handle(row);
                if (written.isPresent()) {
                    seenUris.add(written.get().getCanonicalUri());
                }
            } catch (RuntimeException e) {
                allRowsSuccess = false;
                processingResult.error(row.getId(), e.getMessage());
            }
        }

        if (allRowsSuccess) {
            // un-ActivelyPublisheding disappeared content
            Iterator<Content> allStoredKmContent = contentLister.listContent(ContentListingCriteria.defaultCriteria().forContent(ContentCategory.TOP_LEVEL_ITEM).forPublishers(allKmPublishers).build());
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
