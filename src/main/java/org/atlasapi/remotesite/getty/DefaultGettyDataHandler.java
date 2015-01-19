package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;

public class DefaultGettyDataHandler implements GettyDataHandler {

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentExtractor<VideoResponse, Content> extractor;
    private final ContentMerger contentMerger;

    public DefaultGettyDataHandler(ContentResolver resolver, ContentWriter writer, 
            ContentExtractor<VideoResponse, Content> extractor) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.extractor = checkNotNull(extractor);
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.REPLACE, MergeStrategy.MERGE);
    }

    @Override
    public Identified handle(VideoResponse row) {
        Content content = extractor.extract(row);
        Maybe<Identified> existing = resolve(content.getCanonicalUri());
        if (existing.isNothing()) {
            write(content);
            return content;
        } else {
            Identified identified = existing.requireValue();
            Item result = contentMerger.merge(ContentMerger.asItem(identified), (Item) content);
            write(result);
            return result;
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
