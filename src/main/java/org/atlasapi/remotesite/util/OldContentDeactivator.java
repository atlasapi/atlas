package org.atlasapi.remotesite.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;

/**
 * Many foreign data set ingests into Atlas are of the form of snapshot 
 * of their entire catalogue at that point in time. Content not included 
 * in that snapshot is therefore no longer valid, and needs to be 
 * deactivated in Atlas. This utility class can be used to do that.
 *
 */
public class OldContentDeactivator {

    private static final Logger log = LoggerFactory.getLogger(OldContentDeactivator.class);

    private final ContentLister contentLister;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;

    public OldContentDeactivator(ContentLister contentLister, ContentWriter contentWriter,
            ContentResolver contentResolver) {
        this.contentResolver = checkNotNull(contentResolver);
        this.contentLister = checkNotNull(contentLister);
        this.contentWriter = checkNotNull(contentWriter);
    }
    
    /**
     * Deactivate all content of a publisher, where their URIs are not in a set of
     * currently valid content.
     * 
     * @param publisher     The publisher on which to act 
     * @param validContent  The URIs of all currently valid content
     * @param threshold     Only deactivate any content if at least this percentage of
     *                      content remains in the catalogue. For example, 50 means that
     *                      at least 50% of the content already in Atlas must be in the
     *                      URIs provided in validContent.
     * @return              true iff any content deactivated
     */
    public boolean deactivateOldContent(Publisher publisher, Iterable<String> validContent,
            Integer threshold) {
        ImmutableSet<String> validContentUris = ImmutableSet.copyOf(validContent);
        List<String> toRemove = Lists.newArrayList();
        int allExistingContent = 0;
        Iterator<Content> allContent = contentLister.listContent(
                                            new ContentListingCriteria
                                                    .Builder()
                                                    .forPublisher(publisher)
                                                    .forContent(ImmutableList.copyOf(Sets.union(ContentCategory.CONTAINERS, ContentCategory.ITEMS)))
                                                    .build()
                                                 );
        
        while (allContent.hasNext()) {
            Content content = (Content) allContent.next();
            if (content.isActivelyPublished()) {
                allExistingContent++;
                if (!validContentUris.contains(content.getCanonicalUri())) {
                   toRemove.add(content.getCanonicalUri());
                }
            }
        }
        double seenAsAPercentageOfFullCatalogue
                = allExistingContent > 0  
                     ? (validContentUris.size() / (float) allExistingContent) * 100 
                     : 0;
                     
        if (!toRemove.isEmpty() 
                && (threshold == null || seenAsAPercentageOfFullCatalogue > threshold)) {
            log.info(
                    "Deactivating content for {}, content remaining {}%, threshold {}",
                    new Object[]{
                            publisher,
                            seenAsAPercentageOfFullCatalogue,
                            threshold
                    }

            );
            markAsInactive(toRemove);
            return true;
        }
        log.warn(
                "Content for {} not deactivated, threshold: {}, percentage of content remaining: {}",
                new Object[]{
                        publisher,
                        threshold,
                        Double.valueOf(seenAsAPercentageOfFullCatalogue)
                }
        );
        return false;
    }
    
    private void markAsInactive(Iterable<String> uris) {
        for (String uri : uris) {
            Identified ided = contentResolver.findByCanonicalUris(ImmutableSet.of(uri))
                           .getFirstValue().requireValue();
            if (ided instanceof Content) {
                Content content = (Content) ided;
                content.setActivelyPublished(false);
                update(content);
            }
        }
    }
    
    private void update(Content content) {
        if (content instanceof Container) {
            contentWriter.createOrUpdate((Container) content);
        } else if (content instanceof Item) {
            contentWriter.createOrUpdate((Item) content);
        } else {
            throw new IllegalArgumentException("Content " + content.getCanonicalUri() + " of type " + content.getClass().getName() + " not supported");
        }
    }
}
