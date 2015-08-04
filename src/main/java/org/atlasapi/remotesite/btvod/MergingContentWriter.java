package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;

import static com.google.common.base.Preconditions.checkNotNull;

public class MergingContentWriter {

    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final ContentMerger contentMerger;

    public MergingContentWriter(ContentWriter writer, ContentResolver resolver) {
        this(
                writer,
                resolver,
                new ContentMerger(
                        ContentMerger.MergeStrategy.REPLACE,
                        ContentMerger.MergeStrategy.REPLACE,
                        ContentMerger.MergeStrategy.REPLACE
                )
        );

    }

    public MergingContentWriter(ContentWriter writer, ContentResolver resolver, ContentMerger contentMerger) {
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.contentMerger = contentMerger;

    }

    public void write(Content extracted) {
        Maybe<Identified> existing = resolver
                .findByCanonicalUris(ImmutableSet.of(extracted.getCanonicalUri()))
                .getFirstValue();
        if (existing.hasValue()) {
            Content merged = merge((Content)existing.requireValue(), extracted);
            writeToMongo(merged);
        } else {
            writeToMongo(extracted);
        }
    }

    private Content merge(Content existing, Content extracted) {
        if(extracted instanceof Series) {
            return contentMerger.merge(
                    (Series) existing,
                    (Series) extracted);
        }
        if(extracted instanceof Item) {
            return contentMerger.merge(
                    (Item) existing,
                    (Item) extracted);
        }
        if(extracted instanceof Brand) {
            return contentMerger.merge(
                    (Brand) existing,
                    (Brand) extracted);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unexpected class, expected Series,Brand or Item, got %s",
                        extracted.getClass().getCanonicalName()
                )
        );
    }


    private void writeToMongo(Content content) {
        if (content instanceof Container) {
            writer.createOrUpdate((Container)content);
            return;
        }
        if (content instanceof Item) {
            writer.createOrUpdate((Item)content);
        }
    }

}
