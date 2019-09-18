package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
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
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;


public class LocalOrRemoteNitroFetcher {

    private static final Logger log = LoggerFactory.getLogger(LocalOrRemoteNitroFetcher.class);
    
    private final ContentResolver resolver;
    private final NitroContentAdapter contentAdapter;
    private final ContentMerger contentMerger;
    private final Predicate<Item> fullFetchPermitted;

    public LocalOrRemoteNitroFetcher(ContentResolver resolver, NitroContentAdapter contentAdapter, final Clock clock) {
        this(resolver, contentAdapter,
                new ContentMerger(
                        MergeStrategy.NITRO_VERSIONS_REVOKE,
                        MergeStrategy.KEEP,
                        MergeStrategy.REPLACE
                ),
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

                        return Iterables.any(
                                input.flattenBroadcasts(),
                                new Predicate<org.atlasapi.media.entity.Broadcast>() {

                                    @Override
                                    public boolean apply(
                                            org.atlasapi.media.entity.Broadcast input) {
                                        return fetchForBroadcastsWithin.contains(input.getTransmissionTime());
                                    }
                                }
                        );
                    }

                    /**
                     * Forces a full fetch from nitro if an item with no durations is
                     * encountered
                     *
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
        this(
                resolver,
                contentAdapter,
                new ContentMerger(
                        MergeStrategy.NITRO_VERSIONS_REVOKE,
                        MergeStrategy.KEEP,
                        MergeStrategy.REPLACE
                ),
                fullFetchPermitted
        );
    }
    
    public LocalOrRemoteNitroFetcher(ContentResolver resolver, NitroContentAdapter contentAdapter, 
            ContentMerger contentMerger, Predicate<Item> fullFetchPermitted) {
        this.resolver = checkNotNull(resolver);
        this.contentAdapter = checkNotNull(contentAdapter);
        this.fullFetchPermitted = checkNotNull(fullFetchPermitted);
        this.contentMerger = checkNotNull(contentMerger);
    }

    public ImmutableSet<ModelWithPayload<Item>> resolveItems(Iterable<ModelWithPayload<Item>> items)
            throws NitroException {
        ImmutableSet<String> itemUris = StreamSupport
                .stream(items.spliterator(), false)
                .map(ModelWithPayload::getModel)
                .map(Item::getCanonicalUri)
                .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolvedItems = resolve(itemUris);

        ImmutableSet<ModelWithPayload<Item>> wrappedItems = resolvedItems.getAllResolvedResults().stream()
                .filter(Item.class::isInstance)
                .map(Item.class::cast)
                .map(this::wrapResolvedContentWithEmptyPayload)
                .collect(MoreCollectors.toImmutableSet());

        return mergeItemsWithExisting(ImmutableSet.copyOf(items), wrappedItems);
    }

    public ImmutableSet<ModelWithPayload<Item>> resolveOrFetchItem(
            Iterable<Broadcast> broadcasts)
            throws NitroException {

        if (Iterables.isEmpty(broadcasts)) {
            return ImmutableSet.of();
        }
        Iterable<PidReference> episodeRefs = toEpisodeRefs(broadcasts);
        ImmutableSet<String> itemUris = toItemUris(episodeRefs);
        ResolvedContent resolvedContent = resolve(itemUris);
        ImmutableListMultimap<String, Broadcast> broadcastIndex = buildBroadcastIndex(broadcasts);

        Set<PidReference> toFetch = Sets.newHashSet();
        for (PidReference pidReference : episodeRefs) {
            String uri = toItemUri(pidReference);
            Optional<Identified> id = resolvedContent.asMap().get(uri).toOptional();

            if (!id.isPresent() || fullFetchPermitted.apply((Item) id.get())) {
                log.trace("Will fetch item with PID reference {} Nitro", pidReference.getPid());
                toFetch.add(pidReference);
            }
        }

        Iterable<List<ModelWithPayload<Item>>> fetchedItems =
                contentAdapter.fetchEpisodes(toFetch,broadcastIndex);
        ImmutableSet<ModelWithPayload<Item>> fetchedItemSet =
                ImmutableSet.copyOf(Iterables.concat(fetchedItems));

        ImmutableSet<ModelWithPayload<Item>> wrappedItems = resolvedContent.getAllResolvedResults().stream()
                .filter(Item.class::isInstance)
                .map(Item.class::cast)
                .map(this::wrapResolvedContentWithEmptyPayload)
                .collect(MoreCollectors.toImmutableSet());

        return mergeItemsWithExisting(fetchedItemSet, wrappedItems);
    }

    private <T> ModelWithPayload<T> wrapResolvedContentWithEmptyPayload(T item) {
        return new ModelWithPayload<>(
                item,
                //if item is null ModelWithPayload will throw exception anyway, so no reason to recheck.
                "Resolved content (" + item.getClass().getCanonicalName() + "). Payload is unavailable."
        );
    }

    private ImmutableListMultimap<String, Broadcast> buildBroadcastIndex(
            Iterable<Broadcast> broadcasts
    ) {
        return Multimaps.index(
                broadcasts,
                new Function<Broadcast, String>() {
                    @Override
                    public String apply(Broadcast input) {
                        return NitroUtil.programmePid(input).getPid();
                    }
                }
        );
    }

    private ImmutableSet<ModelWithPayload<Item>> mergeItemsWithExisting(
            Set<ModelWithPayload<Item>> fetchedItems,
            Set<ModelWithPayload<Item>> existingItems) {

        //create an index of the fetched items
        Map<String, ModelWithPayload<Item>> fetchedIndex = LocalOrRemoteNitroFetcher.getIndex(fetchedItems);

        //then iterate the existing items and merge what you can with the fetched
        ImmutableSet.Builder<ModelWithPayload<Item>> merged = ImmutableSet.builder();
        for (ModelWithPayload<Item> existing : existingItems) {
            ModelWithPayload<Item> fetched = fetchedIndex.remove(existing.getModel().getCanonicalUri());
            if (fetched != null) {
                //unwrap for the merger, then rewrap it with the original payload
                Item mergedItem = contentMerger.merge(existing.getModel(), fetched.getModel());
                merged.add(new ModelWithPayload(mergedItem, fetched.getPayload()));
            } else {
                merged.add(existing);
            }
        }

        return ImmutableSet.copyOf(Iterables.concat(merged.build(), fetchedIndex.values()));
    }


    private ResolvedContent resolve(Iterable<String> itemUris) {
        return resolver.findByCanonicalUris(itemUris);
    }

    private ImmutableSet<String> toItemUris(
            Iterable<PidReference> pidRefs) {

        return StreamSupport.stream(pidRefs.spliterator(), false)
                .map(this::toItemUri)
                .collect(MoreCollectors.toImmutableSet());
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

    public ImmutableSet<ModelWithPayload<Container>> resolveOrFetchSeries(
            Iterable<ModelWithPayload<Item>> itemsWithPayload)
            throws NitroException {

        if (Iterables.isEmpty(itemsWithPayload)) {
            return ImmutableSet.of();
        }

        Iterable<Episode> episodes = StreamSupport.stream(itemsWithPayload.spliterator(), false)
                .map(ModelWithPayload::getModel)
                .filter(Episode.class::isInstance)
                .map(Episode.class::cast)
                .collect(MoreCollectors.toImmutableList());

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

        ImmutableSet<ModelWithPayload<Series>> fetched = contentAdapter.fetchSeries(asSeriesPidRefs(toFetch));

        ImmutableSet<ModelWithPayload<Container>> wrappedContainers = resolved.getAllResolvedResults().stream()
                .filter(Container.class::isInstance)
                .map(Container.class::cast)
                .map(this::wrapResolvedContentWithEmptyPayload)
                .collect(MoreCollectors.toImmutableSet());

        return mergeContainersWithExisting(wrappedContainers, fetched);
    }

    private <C extends Container> ImmutableSet<ModelWithPayload<Container>> mergeContainersWithExisting(
            Set<ModelWithPayload<Container>> fetchedContainers,
            Set<ModelWithPayload<C>> existingContainers) {

        //create an index of the fetched items
        Map<String, ModelWithPayload<Container>> fetchedIndex = getIndex(fetchedContainers);

        //then iterate the existing items and merge what you can with the fetched
        ImmutableSet.Builder<ModelWithPayload<Container>> merged = ImmutableSet.builder();
        for (ModelWithPayload<C> existing : existingContainers) {
            ModelWithPayload<Container> fetched = fetchedIndex.remove(existing.getModel().getCanonicalUri());
            if (fetched != null) {
                //unwrap for the merger, then rewrap it with the original payload
                Container mergedContainer = contentMerger.merge(existing.getModel(), fetched.getModel());
                merged.add(new ModelWithPayload(mergedContainer, fetched.getPayload()));
            } else {
                merged.add((ModelWithPayload<Container>)existing);
            }
        }

        return ImmutableSet.copyOf(Iterables.concat(merged.build(), fetchedIndex.values()));

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
    }
    
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

    public ImmutableSet<ModelWithPayload<Container>> resolveOrFetchBrand(
            Iterable<ModelWithPayload<Item>> itemsWithPayload)
            throws NitroException {

        if (Iterables.isEmpty(itemsWithPayload)) {
            return ImmutableSet.of();
        }

        ImmutableList.Builder items = ImmutableList.builder();
        for (ModelWithPayload<Item> itemWithPayload : itemsWithPayload) {
            items.add(itemWithPayload.getModel());
        }

        Multimap<String, Item> brandUriMap = toBrandUriMap(items.build());
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
        
        ImmutableSet<ModelWithPayload<Brand>> fetched = contentAdapter.fetchBrands(asBrandPidRefs(toFetch));

        ImmutableSet<ModelWithPayload<Container>> wrappedContainers = resolved.getAllResolvedResults().stream()
                .filter(Container.class::isInstance)
                .map(Container.class::cast)
                .map(this::wrapResolvedContentWithEmptyPayload)
                .collect(MoreCollectors.toImmutableSet());

        return mergeContainersWithExisting(wrappedContainers, fetched);
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

    /**
     * Get a Map using the {@link Container#TO_URI} as key, and the ModelWithPayload itself as value.
     * The map is mutable, so items can be removed. If duplicate entries are found the later is used.
      */
    public static final <T extends Identified> Map<String, ModelWithPayload<T>> getIndex(
            Set<ModelWithPayload<T>> iter) {

        final boolean[] warning = new boolean[1];
        Map<String, ModelWithPayload<T>> collected
                = StreamSupport.stream(iter.spliterator(), false)
                .collect(Collectors.toMap(
                        mwp -> mwp.getModel().getCanonicalUri(),
                        mwp -> mwp,
                        (mwpExisting, mwpNew) -> {
                            warning[0] = true;
                            return mwpNew;
                        }
                ));
        if (warning[0]) {
            log.warn( "Duplicate keys where found while trying to create an index of Iterables. "
                    + "Only the latest instance was retained.");
        }

        return collected;
    }
    
}
