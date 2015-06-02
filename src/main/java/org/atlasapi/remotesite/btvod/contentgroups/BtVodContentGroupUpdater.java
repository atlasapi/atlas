package org.atlasapi.remotesite.btvod.contentgroups;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.remotesite.btvod.BtVodContentGroupPredicate;
import org.atlasapi.remotesite.btvod.BtVodContentListener;
import org.atlasapi.remotesite.btvod.VodEntryAndContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.base.Maybe;


public class BtVodContentGroupUpdater implements BtVodContentListener {

    private final ContentGroupResolver contentGroupResolver;
    private final ContentGroupWriter contentGroupWriter;
    private final ImmutableMap<String, BtVodContentGroupPredicate> contentGroupsAndCriteria;
    private final String uriPrefix;
    private final Publisher publisher;
    private Multimap<String, ChildRef> contents;

    public BtVodContentGroupUpdater(ContentGroupResolver contentGroupResolver, 
            ContentGroupWriter contentGroupWriter,
            Map<String, BtVodContentGroupPredicate> contentGroupsAndCriteria,
            String uriPrefix, Publisher publisher) {
        this.contentGroupResolver = checkNotNull(contentGroupResolver);
        this.contentGroupWriter = checkNotNull(contentGroupWriter);
        this.contentGroupsAndCriteria = ImmutableMap.copyOf(contentGroupsAndCriteria);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
    }
    
    @Override
    public void onContent(Content content, BtVodEntry vodData) {
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(vodData, content);
        for (Entry<String, BtVodContentGroupPredicate> entry : 
                contentGroupsAndCriteria.entrySet()) {
            if (entry.getValue().apply(vodEntryAndContent)) {
                contents.put(entry.getKey(), content.childRef());
            }
        }
    }

    public void start() {
        contents = HashMultimap.create();
        
        for (BtVodContentGroupPredicate predicate : contentGroupsAndCriteria.values()) {
            predicate.init();
        }
    }
    
    public void finish() {
        for (String key : contentGroupsAndCriteria.keySet()) {
            ContentGroup contentGroup = getOrCreateContentGroup(uriPrefix + key);
            Collection<ChildRef> newChildRefs = contents.get(key);
            
            contentGroup.setContents(newChildRefs);
            contentGroupWriter.createOrUpdate(contentGroup);
        }
    }
    
    private ContentGroup getOrCreateContentGroup(String canonicalUri) {
        Maybe<Identified> maybeContentGroup = contentGroupResolver
                .findByCanonicalUris(ImmutableSet.of(canonicalUri))
                .getFirstValue();
        
        if (maybeContentGroup.hasValue()) {
            return (ContentGroup) maybeContentGroup.requireValue();
        }
        
        return new ContentGroup(canonicalUri, publisher);
    }
    
}
