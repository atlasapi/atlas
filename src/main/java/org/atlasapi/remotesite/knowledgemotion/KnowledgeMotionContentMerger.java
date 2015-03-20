package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;

public class KnowledgeMotionContentMerger implements KnowledgeMotionDataRowHandler {

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentExtractor<KnowledgeMotionDataRow, Optional<? extends Content>> extractor;
    private final ContentMerger contentMerger;

    public KnowledgeMotionContentMerger(ContentResolver resolver, ContentWriter writer,
        ContentExtractor<KnowledgeMotionDataRow, Optional<? extends Content>> extractor) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.extractor = checkNotNull(extractor);
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.REPLACE, MergeStrategy.REPLACE);
    }

    @Override
    public Optional<Content> handle(KnowledgeMotionDataRow row) {
        Content content = extractor.extract(row).orNull();
        if (content == null) {
            return Optional.absent();
        }

        Maybe<Identified> existing = resolve(content.getCanonicalUri());
        if (existing.isNothing()) {
            write(content);
            return Optional.of(content);
        } else {
            Identified identified = existing.requireValue();
            Item merged = contentMerger.merge(ContentMerger.asItem(identified), (Item) content);
            write(merged);
            return Optional.<Content>of(merged);
        }
    }

    public void write(Content content) {
        Item item = (Item) content;
        writer.createOrUpdate(item);
    }

    private Maybe<Identified> resolve(String uri) {
        ImmutableSet<String> uris = ImmutableSet.of(uri);
        return resolver.findByCanonicalUris(uris).get(uri);
    }

}
