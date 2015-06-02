package org.atlasapi.remotesite.btvod.contentgroups;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.remotesite.btvod.BtMpxVodClient;
import org.atlasapi.remotesite.btvod.BtVodContentGroupPredicate;
import org.atlasapi.remotesite.btvod.VodEntryAndContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.portal.PortalClient;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;


public class BtVodContentGroupPredicates {

    private static final String FOX_PROVIDER_ID = "XXA";
    private static final String SONY_PROVIDER_ID = "XXB";
    private static final String EONE_PROVIDER_ID = "XXC";
    private static final String CZN_CONTENT_PROVIDER_ID = "CHC";
    private static final String FILM_CATEGORY = "Film";
    
    public static BtVodContentGroupPredicate schedulerChannelPredicate(final String schedulerChannel) {

        return new BtVodContentGroupPredicate() {

            @Override
            public boolean apply(VodEntryAndContent input) {
                return category.equals(
                        input.getBtVodEntry().getProductType()
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
                            schedulerChannelPredicate(FILM_CATEGORY),
                            Predicates.not(buyToOwnPredicate()),
                            Predicates.not(cznPredicate())
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
        return contentProviderPredicate(CZN_CONTENT_PROVIDER_ID);
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
    
    public static BtVodContentGroupPredicate mpxContentGroupPredicate(final BtMpxVodClient mpxClient, final String feedName) {
        
        return new BtVodContentGroupPredicate() {
            
            private Set<String> ids = null;
            
            @Override
            public boolean apply(VodEntryAndContent input) {
                if (ids == null) {
                    throw new IllegalStateException("Must call init() first");
                }
                return ids.contains(input.getBtVodEntry().getId());
            }
            
            @Override
            public void init() {
                ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                Iterator<BtVodEntry> feed;
                try {
                    feed = mpxClient.getFeed(feedName);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                while (feed.hasNext()) {
                    builder.add(feed.next().getId());
                };
                ids = builder.build();
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
