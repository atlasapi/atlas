package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.portal.PortalClient;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.base.Maybe;


public class BtVodContentGroupUpdater implements BtVodContentListener {

    private static final String FOX_PROVIDER_ID = "XXA";
    private static final String SONY_PROVIDER_ID = "XXB";
    private static final String EONE_PROVIDER_ID = "XXC";
    private static final String CZN_CONTENT_PROVIDER_ID = "CHC";
    private static final String FILM_CATEGORY = "Film";
    
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
    
    public static BtVodContentGroupPredicate categoryPredicate(final String category) {

        return new BtVodContentGroupPredicate() {

            @Override
            public boolean apply(VodEntryAndContent input) {
                return category.equals(
                        input.getBtVodEntry().getBtproduct$productType()
                );
            }

            @Override
            public void init() {

            }
        };
    }

    public static BtVodContentGroupPredicate contentProviderPredicate(final String providerId) {
        
        return new BtVodContentGroupPredicate() {

            @Override
            public boolean apply(VodEntryAndContent input) {
                return providerId.equals(
                                    input.getBtVodEntry()
                                         .getContentProviderId()
                );
            }
            
            @Override
            public void init() {
                
            }
        };
    }
    
    @SuppressWarnings("unchecked")
    public static BtVodContentGroupPredicate buyToOwnPredicate() {
        
        return new BtVodContentGroupPredicate() {
            
            private final Predicate<VodEntryAndContent> delegate =
                    Predicates.or(
                            contentProviderPredicate(FOX_PROVIDER_ID),
                            contentProviderPredicate(SONY_PROVIDER_ID),
                            contentProviderPredicate(EONE_PROVIDER_ID));
            
            @Override
            public boolean apply(VodEntryAndContent input) {
                return delegate.apply(input);
            }
            
            @Override
            public void init() {
                
            }
        };
    };
    
    public static BtVodContentGroupPredicate filmPredicate() {
        
        return new BtVodContentGroupPredicate() {
            
            @SuppressWarnings("unchecked")
            private final Predicate<VodEntryAndContent> delegate =
                    Predicates.and(
                            BtVodContentGroupUpdater.categoryPredicate(FILM_CATEGORY),
                            Predicates.not(BtVodContentGroupUpdater.buyToOwnPredicate()),
                            Predicates.not(BtVodContentGroupUpdater.cznPredicate())
                    );
            
            @Override
            public boolean apply(VodEntryAndContent input) {
                return delegate.apply(input);
            }
            
            @Override
            public void init() {
                
            }
        };
    }
    
    public static BtVodContentGroupPredicate cznPredicate() {
        return BtVodContentGroupUpdater.contentProviderPredicate(CZN_CONTENT_PROVIDER_ID);
    }
    
    public static BtVodContentGroupPredicate portalContentGroupPredicate(final PortalClient portalClient, final String groupId,
            @Nullable final Class<? extends Described> typeFilter) {
        
        return new BtVodContentGroupPredicate() {
            
            private Set<String> ids = null;
            
            @Override
            public boolean apply(VodEntryAndContent input) {
                if (ids == null) {
                    throw new IllegalStateException("Must call init() first");
                }
                return ids.contains(input.getBtVodEntry().getGuid())
                       && (typeFilter == null
                               || typeFilter.isAssignableFrom(input.getContent().getClass()));
            }
            
            @Override
            public void init() {
                ids = portalClient.getProductIdsForGroup(groupId).or(ImmutableSet.<String>of());
            }
        };
    }
    
    @SuppressWarnings("unchecked")
    public static Predicate<VodEntryAndContent> boxOfficePredicate() {
        
        return Predicates.and(
                Predicates.not(buyToOwnPredicate()),
                Predicates.not(cznPredicate()), 
                new Predicate<VodEntryAndContent>() {

                    @Override
                    public boolean apply(VodEntryAndContent input) {
                        return input.getContent() instanceof Film;
                    }
        });
                
    }
}
