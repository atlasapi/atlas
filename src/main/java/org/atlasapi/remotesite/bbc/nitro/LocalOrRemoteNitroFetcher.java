package org.atlasapi.remotesite.bbc.nitro;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;

import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalOrRemoteNitroFetcher {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ContentResolver resolver;
    private final NitroContentAdapter contentAdapter;
    private final ContentMerger contentMerger;
    private final Predicate<Item> fullFetchPermitted;

    public LocalOrRemoteNitroFetcher(ContentResolver resolver, NitroContentAdapter contentAdapter, final Clock clock) {
        this(resolver, contentAdapter, 
                new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE),
                new Predicate<Item>() {

                    @Override
                    public boolean apply(Item input) {
                        if (hasVersionsWithNoDurations(input)) {                        
                            return true;
                        }
                        
                        LocalDate today = clock.now().toLocalDate();
                        
                        // radio are more likely to publish clips after a show has been broadcast
                        // so with a limited ingest window it is more important to go back as far as possible for radio
                        // to ensure that clips are not missed
                        // tv has a longer forward interval, to ensure for repeated shows that we refetch everything, to make sure
                        // we pull in all changes on a given programme even for later repeats of something broadcast earlier.
                        final Interval fetchForBroadcastsWithin = 
                                MediaType.AUDIO.equals(input.getMediaType()) 
                                    ? broadcastInterval(today.minusDays(5), today.plusDays(1)) 
                                    : broadcastInterval(today.minusDays(3), today.plusDays(10));
                        
                        return Iterables.any(input.flattenBroadcasts(), new Predicate<org.atlasapi.media.entity.Broadcast>() {

                            @Override
                            public boolean apply(org.atlasapi.media.entity.Broadcast input) {
                                return fetchForBroadcastsWithin.contains(input.getTransmissionTime());
                            }});
                    }

                    /**
                     * Forces a full fetch from nitro if an item with no durations is encountered
                     * @param input
                     * @return
                     */
                    private boolean hasVersionsWithNoDurations(Item input) {
                        return Iterables.any(input.getVersions(), new Predicate<Version>() {
                            @Override
                            public boolean apply(Version input) {
                                return input.getDuration() == null;
                            }
                        });
                    }
            
                    }
                );
    }
    
    private static Interval broadcastInterval(LocalDate start, LocalDate end) {
        return new Interval(start.toDateTimeAtStartOfDay(DateTimeZone.UTC), end.toDateTimeAtStartOfDay(DateTimeZone.UTC));
    }
    
    public LocalOrRemoteNitroFetcher(ContentResolver resolver, NitroContentAdapter contentAdapter, 
            Predicate<Item> fullFetchPermitted) {
        this(resolver, contentAdapter, new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE), fullFetchPermitted);
    }
    
    public LocalOrRemoteNitroFetcher(ContentResolver resolver, NitroContentAdapter contentAdapter, 
            ContentMerger contentMerger, Predicate<Item> fullFetchPermitted) {
        this.resolver = checkNotNull(resolver);
        this.contentAdapter = checkNotNull(contentAdapter);
        this.fullFetchPermitted = checkNotNull(fullFetchPermitted);
        this.contentMerger = checkNotNull(contentMerger);
    }

    public ResolveOrFetchResult<Item> resolveItems(Iterable<Item> items)
            throws NitroException {
        ImmutableList.Builder<String> itemUris = ImmutableList.builder();
        for (Item item : items) {
            itemUris.add(item.getCanonicalUri());
        }
        ResolvedContent resolvedItems = resolve(itemUris.build());

        return mergeItemsWithExisting(
                ImmutableSet.copyOf(items),
                ImmutableSet.copyOf(Iterables.filter(resolvedItems.getAllResolvedResults(), Item.class)));
    }

    public ResolveOrFetchResult<Item> resolveOrFetchItem(Iterable<Broadcast> broadcasts)
            throws NitroException {
        if (Iterables.isEmpty(broadcasts)) {
            return ResolveOrFetchResult.empty();
        }
        Iterable<PidReference> episodeRefs = toEpisodeRefs(broadcasts);
        ImmutableSet<String> itemUris = toItemUris(episodeRefs);
        ResolvedContent resolvedItems = resolve(itemUris);
        
        Set<PidReference> toFetch = Sets.newHashSet();
        for (PidReference pidReference : episodeRefs) {
            Maybe<Identified> maybeId = resolvedItems.asMap().get(toItemUri(pidReference));
            
            if (!maybeId.hasValue()
                    || fullFetchPermitted.apply((Item)maybeId.requireValue())) {
                log.trace("Will fetch item with PID reference {} Nitro", pidReference.getPid());
                toFetch.add(pidReference);
            }
        }

        Iterable<Item> fetched = contentAdapter.fetchEpisodes(toFetch);
        return mergeItemsWithExisting(fetched, ImmutableSet.copyOf(Iterables.filter(resolvedItems.getAllResolvedResults(), Item.class)));
    }
    
    private ResolveOrFetchResult<Item> mergeItemsWithExisting(Iterable<Item> fetchedItems,
            Set<Item> existingItems) {
        Map<String, Item> fetchedIndex = Maps.newHashMap(Maps.uniqueIndex(fetchedItems, Identified.TO_URI));
        ImmutableSet.Builder<Item> resolved = ImmutableSet.builder();
        for (Item existing : existingItems) {
            Item fetched = fetchedIndex.remove(existing.getCanonicalUri());
            if (fetched != null) {
                resolved.add(contentMerger.merge((Item) existing, (Item) fetched));
            } else {
                resolved.add(existing);
            }
            
        }
        return new ResolveOrFetchResult<>(resolved.build(), fetchedIndex.values());
    }


    private ResolvedContent resolve(Iterable<String> itemUris) {
        return resolver.findByCanonicalUris(itemUris);
    }

    private ImmutableSet<String> toItemUris(Iterable<PidReference> pidRefs) {
        return ImmutableSet.copyOf(Iterables.transform(pidRefs, new Function<PidReference, String>() {
            @Override
            public String apply(PidReference input) {
                return toItemUri(input);
            }
        }));
    }
    
    private String toItemUri(PidReference pidReference) {
        return BbcFeeds.nitroUriForPid(pidReference.getPid());
    }

    private Iterable<PidReference> toEpisodeRefs(Iterable<Broadcast> broadcasts) {
        return Iterables.filter(Iterables.transform(broadcasts, new Function<Broadcast, PidReference>() {
            @Override
            public PidReference apply(Broadcast input) {
                final PidReference pidRef = NitroUtil.programmePid(input);
                if (pidRef == null) {
                    log.warn("No programme pid for broadcast {}", input.getPid());
                    return null;
                }
                return pidRef;
            }
        }), Predicates.notNull());
    }
    
    public ImmutableSet<Container> resolveOrFetchSeries(Iterable<Item> items) throws NitroException {
        if (Iterables.isEmpty(items)) {
            return ImmutableSet.of();
        }
        Iterable<Episode> episodes = Iterables.filter(items, Episode.class);
        Multimap<String, Episode> seriesUriMap = toSeriesUriMap(episodes);
        Set<String> seriesUris = seriesUriMap.keySet();
        ResolvedContent resolved = resolver.findByCanonicalUris(seriesUris);
        
        Set<String> toFetch = Sets.newHashSet();
        for (String seriesUri : seriesUris) {
            Maybe<Identified> maybeId = resolved.asMap().get(seriesUri);
            
            if (!maybeId.hasValue()
                    || Iterables.any(seriesUriMap.get(seriesUri), fullFetchPermitted)) {
                log.trace("Will fetch series {} from Nitro", seriesUri);
                toFetch.add(seriesUri);
            }
        }
        
        ImmutableSet<Series> fetched = contentAdapter.fetchSeries(asSeriesPidRefs(toFetch));
        
        return mergeContainersWithExisting(
                    fetched, 
                    ImmutableSet.copyOf(Iterables.filter(resolved.getAllResolvedResults(), Container.class))).getAll();
    }
    
    private ResolveOrFetchResult<Container> mergeContainersWithExisting(ImmutableSet<? extends Container> fetchedContainers,
            Set<? extends Container> existingContainers) {
        Map<String, Container> fetchedIndex = Maps.newHashMap(Maps.uniqueIndex(fetchedContainers, Identified.TO_URI));
        ImmutableSet.Builder<Container> resolved = ImmutableSet.builder();
        for (Container existing : existingContainers) {
            Container fetched = fetchedIndex.remove(existing.getCanonicalUri());
            if (fetched != null) {
                resolved.add(contentMerger.merge((Container) existing, (Container) fetched));
            } else {
                resolved.add(existing);
            }
            
        }
        return new ResolveOrFetchResult<>(resolved.build(), fetchedIndex.values());
    }

    private Iterable<PidReference> asSeriesPidRefs(Iterable<String> pids) {
        return asTypePidsRefs(pids, "series");
    }

    private Iterable<PidReference> asTypePidsRefs(Iterable<String> pids, final String type) {
        return Iterables.transform(pids, new Function<String, PidReference>(){
            @Override
            public PidReference apply(String input) {
                PidReference pidRef = new PidReference();
                pidRef.setPid(BbcFeeds.pidFrom(input));
                pidRef.setResultType(type);
                return pidRef;
            }});
    }

    private Multimap<String, Episode> toSeriesUriMap(Iterable<Episode> episodes) {
        return Multimaps.index(Iterables.filter(episodes, HAS_SERIES_REF), TO_SERIES_REF_URI); 
    };
    
    private static Function<Episode, String> TO_SERIES_REF_URI = new Function<Episode, String>() {

        @Override
        public String apply(Episode input) {
            return input.getSeriesRef().getUri();
        }
    };
    
    private static Predicate<Episode> HAS_SERIES_REF = new Predicate<Episode>() {

        @Override
        public boolean apply(Episode input) {
            return input.getSeriesRef() != null;
        }
        
    };

    public ImmutableSet<Container> resolveOrFetchBrand(Iterable<Item> items) throws NitroException {
        if (Iterables.isEmpty(items)) {
            return ImmutableSet.of();
        }
        Multimap<String, Item> brandUriMap = toBrandUriMap(items);
        Set<String> brandUris = brandUriMap.keySet();
        
        ResolvedContent resolved = resolver.findByCanonicalUris(brandUris);
        Set<String> toFetch = Sets.newHashSet();
        for (String brandUri : brandUris) {
            Maybe<Identified> maybeId = resolved.asMap().get(brandUri);
            
            if (!maybeId.hasValue()
                    || Iterables.any(brandUriMap.get(brandUri), fullFetchPermitted)) {
                log.trace("Will fetch brand {} from Nitro", brandUri);
                toFetch.add(brandUri);
            }
        }
        
        ImmutableSet<Brand> fetched = contentAdapter.fetchBrands(asBrandPidRefs(toFetch));
        return mergeContainersWithExisting(
                    fetched, 
                    ImmutableSet.copyOf(Iterables.filter(resolved.getAllResolvedResults(), Container.class))).getAll();
    }
    
    private Multimap<String, Item> toBrandUriMap(Iterable<Item> items) {
        return Multimaps.index(Iterables.filter(items, HAS_BRAND), TO_BRAND_REF_URI);
    }

    private static Function<Item, String> TO_BRAND_REF_URI = new Function<Item, String>() {

        @Override
        public String apply(Item input) {
            return input.getContainer() == null ? null : input.getContainer().getUri();
        }
    };
    
    private Iterable<PidReference> asBrandPidRefs(Iterable<String> uris) {
        return asTypePidsRefs(uris, "brand");
    }

    private static Predicate<Item> HAS_BRAND = new Predicate<Item>() {

        @Override
        public boolean apply(Item input) {
            return (!inTopLevelSeries(input)) && input.getContainer() != null;
        }
        
    };

    private static boolean inTopLevelSeries(Item item) {
        if (item instanceof Episode) {
            Episode ep = (Episode)item;
            return ep.getSeriesRef() != null 
                && ep.getSeriesRef().equals(ep.getContainer());
        }
        return false;
    }
    
}
