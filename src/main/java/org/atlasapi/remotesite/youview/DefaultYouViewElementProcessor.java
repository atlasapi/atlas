package org.atlasapi.remotesite.youview;

import nu.xom.Element;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;


public class DefaultYouViewElementProcessor implements YouViewElementProcessor {

    private final YouViewContentExtractor extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentMerger contentMerger;
    
    public DefaultYouViewElementProcessor(YouViewContentExtractor extractor, ContentResolver resolver, ContentWriter writer) {
        this.extractor = extractor;
        this.resolver = resolver;
        this.writer = writer;
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.MERGE);
    }
    
    @Override
    public ItemRefAndBroadcast process(Publisher targetPublisher, Element element) {
        Item item = extractor.extract(targetPublisher, element);
        removeStaleScheduleEventOnOldItems();
        Maybe<Identified> existing = resolve(item.getCanonicalUri());
        if (existing.isNothing()) {
            write(item);
        } else {
            Identified identified = existing.requireValue();
                write(contentMerger.merge(ContentMerger.asItem(identified), item));
        }
        return new ItemRefAndBroadcast(item, getBroadcastFromItem(item));
    }

    private Broadcast getBroadcastFromItem(Item item) {
        Version version = Iterables.getOnlyElement(item.getVersions());
        return Iterables.getOnlyElement(version.getBroadcasts());
    }

    private Maybe<Identified> resolve(String uri) {
        ImmutableSet<String> uris = ImmutableSet.of(uri);
        return resolver.findByCanonicalUris(uris).get(uri);
    }

    private void write(Content content) {
        writer.createOrUpdate((Item) content);
    }
    
    private void removeStaleScheduleEventOnOldItems() {
        //TODO
    }
}
