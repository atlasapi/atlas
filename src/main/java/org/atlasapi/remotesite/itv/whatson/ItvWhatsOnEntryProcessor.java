package org.atlasapi.remotesite.itv.whatson;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


public class ItvWhatsOnEntryProcessor {
    
    private final ItvWhatsOnEntryExtractor extractor;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final ContentMerger contentMerger;
  
    public ItvWhatsOnEntryProcessor(ContentResolver contentResolver, 
            ContentWriter contentWriter, ChannelResolver channelResolver,
            ItvWhatsOnLocationPolicyIds locationPolicyIds) {
        this.extractor = new ItvWhatsOnEntryExtractor(new ItvWhatsonChannelMap(channelResolver),
                locationPolicyIds);
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    private Content createOrUpdate(Content content) {
        ResolvedContent resolved = contentResolver.findByCanonicalUris(ImmutableList.of(content.getCanonicalUri()));
        if (!resolved.resolved(content.getCanonicalUri())) {
            return create(content);
        } else {
           return update(content, resolved);
        }
    }
    
    public Content create(Content content) {
        if (content instanceof Item) {
            contentWriter.createOrUpdate((Item) content);
        } else {
            contentWriter.createOrUpdate((Container) content); 
        }
        return content;
    }
    
    public Content update(Content content, ResolvedContent resolved) {
        Content resolvedContent = (Content)resolved.get(content.getCanonicalUri()).requireValue();
        if (content instanceof Item) {
            Item resolvedItem = (Item)resolvedContent;
            Item contentItem = (Item)content;
            
            // The API sometimes fails to return a container for an episode.
            // If that happens we don't want to remove an existing reference.
            if (resolvedItem.getContainer() != null) {
                contentItem.setParentRef(resolvedItem.getContainer());
            }
            contentWriter.createOrUpdate(contentMerger.merge(resolvedItem, contentItem));
            return resolvedItem;
        } else {
            // Only title and uri of brands and series is provided
            // Keep titles as is as they can change in a single run to prevent flip-flopping
            return resolvedContent;
        }
    }
    
    public void createOrUpdateAtlasEntityFrom(ItvWhatsOnEntry entry) {
        Item item = new Item();
        Optional<Series> series = extractor.toSeries(entry);
        if (series.isPresent()) {
            Series resolvedSeries = (Series) createOrUpdate(series.get());
            Episode epsiode = new Episode();
            epsiode.setSeries(resolvedSeries);
            item = epsiode;
        }
        Optional<Brand> brand = extractor.toBrand(entry);
        if (brand.isPresent()) {
            Brand resolvedBrand = (Brand) createOrUpdate(brand.get());
            item.setContainer(resolvedBrand);
        }
        extractor.setCommonItemAttributes(item, entry);
        createOrUpdate(item);
    }
}
