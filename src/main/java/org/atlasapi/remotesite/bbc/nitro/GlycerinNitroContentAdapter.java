package org.atlasapi.remotesite.bbc.nitro;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.BroadcastsQuery;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBrandExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroSeriesExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.ANCESTOR_TITLES;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.CONTRIBUTIONS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.GENRE_GROUPINGS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.IMAGES;

/**
 * A {@link NitroContentAdapter} based on a {@link Glycerin}.
 */
public class GlycerinNitroContentAdapter implements NitroContentAdapter {

    private static final int NITRO_BATCH_SIZE = 10;

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroContentAdapter.class);

    private final Glycerin glycerin;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    private final NitroBrandExtractor brandExtractor;
    private final NitroSeriesExtractor seriesExtractor;
    private final NitroEpisodeExtractor itemExtractor;
    private final int pageSize;

    private final ListeningExecutorService executor;
    private final MetricRegistry metricRegistry;
    private final String metricPrefix;

    public GlycerinNitroContentAdapter(
            Glycerin glycerin,
            GlycerinNitroClipsAdapter clipsAdapter,
            QueuingPersonWriter peopleWriter,
            Clock clock,
            int pageSize,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        this.glycerin = checkNotNull(glycerin);
        this.pageSize = pageSize;
        this.clipsAdapter = checkNotNull(clipsAdapter);
        this.brandExtractor = new NitroBrandExtractor(clock);
        this.seriesExtractor = new NitroSeriesExtractor(clock);
        this.itemExtractor = new NitroEpisodeExtractor(clock, peopleWriter);
        this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(60));
        this.metricRegistry = metricRegistry;
        this.metricPrefix = metricPrefix;
    }

    @Override
    public ImmutableSet<Brand> fetchBrands(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "brand");
            ImmutableList<Programme> programmes =
                    fetchProgrammes(makeProgrammeQueries(refs));
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<Brand> fetched = ImmutableSet.builder();
            for (Programme programme : programmes) {
                if (programme.isBrand()) {
                    Brand brand = brandExtractor.extract(programme.getAsBrand());
                    brand.setClips(clips.get(brand.getCanonicalUri()));
                    fetched.add(brand);
                }
            }
            return fetched.build();
        } catch (GlycerinException e) {
            throw new NitroException(NitroUtil.toPids(refs).toString(), e);
        }
    }

    private void checkRefType(Iterable<PidReference> refs, String type) {
        for (PidReference ref : refs) {
            checkArgument(type.equals(ref.getResultType()), "%s not %s", ref.getPid(), type);
        }
    }

    @Override
    public ImmutableSet<Series> fetchSeries(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "series");
            ImmutableList<Programme> programmes =
                    fetchProgrammes(makeProgrammeQueries(refs));
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<Series> fetched = ImmutableSet.builder();
            for (Programme programme : programmes) {
                if (programme.isSeries()) {
                    Series series = seriesExtractor.extract(programme.getAsSeries());
                    series.setClips(clips.get(series.getCanonicalUri()));
                    fetched.add(series);
                }
            }
            return fetched.build();
        } catch (GlycerinException e) {
            throw new NitroException(NitroUtil.toPids(refs).toString(), e);
        }
    }

    public Iterable<ProgrammesQuery> makeProgrammeQueries(Iterable<PidReference> refs) {
        ImmutableList.Builder<ProgrammesQuery> queries = ImmutableList.builder();

        for (List<PidReference> ref : Iterables.partition(refs, NITRO_BATCH_SIZE)) {
            ProgrammesQuery query = ProgrammesQuery.builder()
                    .withPid(toStrings(ref))
                    .withMixins(ANCESTOR_TITLES, CONTRIBUTIONS, IMAGES, GENRE_GROUPINGS)
                    .withPageSize(pageSize)
                    .build();

            queries.add(query);
        }

        return queries.build();
    }

    @Override
    public ImmutableSet<Item> fetchEpisodes(ProgrammesQuery programmesQuery) throws NitroException {
        try {
            ImmutableList<Programme> programmes = fetchProgrammes(ImmutableList.of(programmesQuery));

            if (programmes.isEmpty()) {
                log.warn("No programmes found for queries {}", programmesQuery.toString());
                return ImmutableSet.of();
            }

            return fetchEpisodesFromProgrammes(programmes);
        } catch (GlycerinException e) {
            throw new NitroException(programmesQuery.toString(), e);
        }
    }

    @Override
    public ImmutableSet<Item> fetchEpisodes(Iterable<PidReference> refs)
            throws NitroException {
        try {
            Iterable<ProgrammesQuery> programmesQueries = makeProgrammeQueries(refs);
            ImmutableList<Programme> programmes = fetchProgrammes(programmesQueries);

            if (programmes.isEmpty()) {
                log.warn("No programmes found for queries {}", programmesQueries.toString());
                return ImmutableSet.of();
            }

            return fetchEpisodesFromProgrammes(programmes);
        } catch (GlycerinException e) {
            throw new NitroException(refs.toString(), e);
        }
    }

    private ImmutableSet<Item> fetchEpisodesFromProgrammes(ImmutableList<Programme> programmes)
            throws NitroException, GlycerinException {
        ImmutableList<Episode> episodes = getAsEpisodes(programmes);
        ImmutableList<NitroItemSource<Episode>> sources = toItemSources(episodes);

        /* TODO: this is a fugly-ish hack, because while in reality the clipsAdapter only needs
                 the PIDs in string form, the public interface only takes PidRef's, which aren't
                 available here. */
        Iterable<PidReference> episodeRefs = Iterables.transform(episodes,
                input -> {
                    PidReference pidReference = new PidReference();
                    pidReference.setHref(input.getUri());
                    pidReference.setPid(input.getPid());
                    return pidReference;
                });

        Multimap<String, Clip> clips = clipsAdapter.clipsFor(episodeRefs);

        ImmutableSet.Builder<Item> fetched = ImmutableSet.builder();
        for (NitroItemSource<Episode> source : sources) {
            Item item = itemExtractor.extract(source);
            item.setClips(clips.get(item.getCanonicalUri()));
            fetched.add(item);
        }
        return fetched.build();
    }

    private ImmutableList<NitroItemSource<Episode>> toItemSources(ImmutableList<Episode> episodes)
            throws GlycerinException, NitroException {
        ListMultimap<String, Availability> availabilities = availabilities(episodes);
        ListMultimap<String, Broadcast> broadcasts = broadcasts(episodes);
        ListMultimap<String, Version> versions = versions(episodes);
        ImmutableList.Builder<NitroItemSource<Episode>> sources = ImmutableList.builder();
        for (Episode episode : episodes) {
            sources.add(NitroItemSource.valueOf(
                    episode,
                    availabilities.get(episode.getPid()),
                    broadcasts.get(episode.getPid()),
                    versions.get(episode.getPid())));
        }
        return sources.build();
    }

    private ImmutableList<Episode> getAsEpisodes(ImmutableList<Programme> programmes) {
        return programmes.stream()
                .map(programme -> {
                    if (programme.isEpisode()) {
                        return programme.getAsEpisode();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableList());
    }

    private ImmutableList<Programme> fetchProgrammes(Iterable<ProgrammesQuery> queries)
            throws GlycerinException {

        List<ListenableFuture<ImmutableList<Programme>>> futures = Lists.newArrayList();

        for (ProgrammesQuery query : queries) {
            futures.add(executor.submit(exhaustingProgrammeCallable(query)));
        }

        ListenableFuture<List<ImmutableList<Programme>>> all = Futures.allAsList(futures);
        try {
            return ImmutableList.copyOf(Iterables.concat(all.get()));
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }

    private <T> ImmutableList<T> exhaust(GlycerinResponse<T> resp) throws GlycerinException {
        ImmutableList.Builder<T> programmes = ImmutableList.builder();
        ImmutableList<T> results = resp.getResults();
        programmes.addAll(results);
        while (resp.hasNext()) {
            resp = resp.getNext();
            programmes.addAll(resp.getResults());
        }
        return programmes.build();
    }

    private Iterable<String> toStrings(Iterable<PidReference> refs) {
        return Iterables.transform(refs, PidReference::getPid);
    }

    private ListMultimap<String, Broadcast> broadcasts(ImmutableList<Episode> episodes)
            throws GlycerinException {
        List<ListenableFuture<ImmutableList<Broadcast>>> futures = Lists.newArrayList();

        for (List<Episode> episode : Iterables.partition(episodes, NITRO_BATCH_SIZE)) {
            BroadcastsQuery query = BroadcastsQuery.builder()
                    .withDescendantsOf(toPids(episode))
                    .withPageSize(pageSize)
                    .build();
            futures.add(executor.submit(exhaustingBroadcastsCallable(query)));
        }
        ListenableFuture<List<ImmutableList<Broadcast>>> successful = Futures.allAsList(futures);
        try {
            return Multimaps.index(Iterables.concat(
                    successful.get()),
                    broadcast -> NitroUtil.programmePid(broadcast).getPid()
            );
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }

    private ListMultimap<String, Version> versions(ImmutableList<Episode> episodes)
            throws GlycerinException {
        List<ListenableFuture<ImmutableList<Version>>> futures = Lists.newArrayList();

        for (List<Episode> episode : Iterables.partition(episodes, NITRO_BATCH_SIZE)) {
            VersionsQuery query = VersionsQuery.builder()
                    .withDescendantsOf(toPids(episode))
                    .withPageSize(pageSize)
                    .build();
            futures.add(executor.submit(exhaustingVersionsCallable(query)));
        }

        ListenableFuture<List<ImmutableList<Version>>> all = Futures.allAsList(futures);
        try {
            return Multimaps.index(Iterables.concat(
                    all.get()),
                    version -> NitroUtil.programmePid(version).getPid()
            );
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }

    private ListMultimap<String, Availability> availabilities(ImmutableList<Episode> episodes)
            throws GlycerinException {
        if (episodes.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        List<ListenableFuture<ImmutableList<Availability>>> futures = Lists.newArrayList();

        for (List<Episode> episode : Iterables.partition(episodes, NITRO_BATCH_SIZE)) {
            AvailabilityQuery query = AvailabilityQuery.builder()
                    .withDescendantsOf(toPids(episode))
                    .withPageSize(pageSize)
                    .withMediaSet(
                            "apple-iphone4-ipad-hls-3g",
                            "apple-iphone4-hls",
                            "pc",
                            "iptv-all",
                            "captions"
                    )
                    .build();

            futures.add(executor.submit(exhaustingAvailabilityCallable(query)));
        }

        ListenableFuture<List<ImmutableList<Availability>>> all = Futures.allAsList(futures);
        Iterable<Availability> list = null;
        try {
            list = Iterables.concat(all.get());
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }

        return Multimaps.index(list, NitroUtil::programmePid);
    }

    private Callable<ImmutableList<Availability>> exhaustingAvailabilityCallable(
            final AvailabilityQuery query
    ) {

        return () ->
        {
            Timer.Context timer = metricRegistry.timer(metricPrefix + "glycerin.availability").time();
            ImmutableList availabilities = exhaust(glycerin.execute(query));
            timer.stop();
            return availabilities;
        };
    }

    private Callable<ImmutableList<Version>> exhaustingVersionsCallable(final VersionsQuery query) {

        return () ->
        {
            Timer.Context timer = metricRegistry.timer(metricPrefix + "glycerin.version").time();
            ImmutableList availabilities = exhaust(glycerin.execute(query));
            timer.stop();
            return availabilities;
        };
    }

    private Callable<ImmutableList<Broadcast>> exhaustingBroadcastsCallable(
            final BroadcastsQuery query
    ) {
        return () ->
        {
            Timer.Context timer = metricRegistry.timer(metricPrefix + "glycerin.broadcast").time();
            ImmutableList availabilities = exhaust(glycerin.execute(query));
            timer.stop();
            return availabilities;
        };
    }

    private Callable<ImmutableList<Programme>> exhaustingProgrammeCallable(
            final ProgrammesQuery query
    ) {
        return () ->
        {
            Timer.Context timer = metricRegistry.timer(metricPrefix + "glycerin.programme").time();
            ImmutableList availabilities = exhaust(glycerin.execute(query));
            timer.stop();
            return availabilities;
        };
    }

    private Iterable<String> toPids(List<Episode> episodes) {
        return Iterables.transform(episodes, Episode::getPid);
    }

}
