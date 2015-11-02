package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

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
import org.atlasapi.remotesite.bbc.nitro.v1.NitroClient;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroGenreGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
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
import com.metabroadcast.common.time.Clock;

/**
 * A {@link NitroContentAdapter} based on a {@link Glycerin}.
 *
 */
public class GlycerinNitroContentAdapter implements NitroContentAdapter {

    private static final int NITRO_BATCH_SIZE = 10;

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroContentAdapter.class);

    private final Glycerin glycerin;
    private final GlycerinNitroClipsAdapter clipsAdapter;
    private final NitroClient nitroClient;

    private final NitroBrandExtractor brandExtractor;
    private final NitroSeriesExtractor seriesExtractor;
    private final NitroEpisodeExtractor itemExtractor;
    private final int pageSize;

    private final ListeningExecutorService executor;

    public GlycerinNitroContentAdapter(Glycerin glycerin, NitroClient nitroClient, GlycerinNitroClipsAdapter clipsAdapter, QueuingPersonWriter peopleWriter, Clock clock, int pageSize) {
        this.glycerin = checkNotNull(glycerin);
        this.nitroClient = checkNotNull(nitroClient);
        this.pageSize = pageSize;
        this.clipsAdapter = checkNotNull(clipsAdapter);
        this.brandExtractor = new NitroBrandExtractor(clock);
        this.seriesExtractor = new NitroSeriesExtractor(clock);
        this.itemExtractor = new NitroEpisodeExtractor(clock, peopleWriter);
        this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(60));
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
                    .withMixins(TITLES, PEOPLE)
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

        // TODO: track down why this NPEs
        Iterable<PidReference> episodeRefs = Iterables.transform(episodes,
                new Function<Episode, PidReference>() {
                    @Override
                    public PidReference apply(Episode input) {
                        Episode.Version version = input.getVersion();
                        PidReference pidReference = new PidReference();
                        pidReference.setHref(version.getHref());
                        pidReference.setPid(version.getPid());
                        return pidReference;
                    }
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
                    genres(episode),
                    versions.get(episode.getPid())));
        }
        return sources.build();
    }

    private ImmutableList<Episode> getAsEpisodes(ImmutableList<Programme> programmes) {
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(programmes,
                new Function<Programme, Episode>() {
                    @Override
                    public Episode apply(Programme input) {
                        if (input.isEpisode()) {
                            return input.getAsEpisode();
                        }
                        return null;
                    }
                }), Predicates.notNull()));
    }

    private ImmutableList<Programme> fetchProgrammes(Iterable<ProgrammesQuery> queries) throws GlycerinException {

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
        while(resp.hasNext()) {
            resp = resp.getNext();
            programmes.addAll(resp.getResults());
        }
        return programmes.build();
    }

    private Iterable<String> toStrings(Iterable<PidReference> refs) {
        return Iterables.transform(refs, new Function<PidReference, String>() {
            @Override
            public String apply(PidReference input) {
                return input.getPid();
            }
        });
    }

    private List<NitroGenreGroup> genres(Episode episode) throws NitroException {
        return nitroClient.genres(episode.getPid());
    }

    private ListMultimap<String, Broadcast> broadcasts(ImmutableList<Episode> episodes) throws GlycerinException {
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
            return Multimaps.index(Iterables.concat(successful.get()), new Function<Broadcast, String>() {
                @Override
                public String apply(Broadcast input) {
                    return NitroUtil.programmePid(input).getPid();
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }

    private ListMultimap<String, Version> versions(ImmutableList<Episode> episodes) throws GlycerinException {
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
            return Multimaps.index(Iterables.concat(all.get()), new Function<Version, String>() {
                @Override
                public String apply(Version input) {
                    return NitroUtil.programmePid(input).getPid();
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }

    private ListMultimap<String, Availability> availabilities(ImmutableList<Episode> episodes) throws GlycerinException {
        if (episodes.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        List<ListenableFuture<ImmutableList<Availability>>> futures = Lists.newArrayList();

        for (List<Episode> episode : Iterables.partition(episodes, NITRO_BATCH_SIZE)) {
            AvailabilityQuery query = AvailabilityQuery.builder()
                    .withDescendantsOf(toPids(episode))
                    .withPageSize(pageSize)
                    .withMediaSet("apple-iphone4-ipad-hls-3g", "apple-iphone4-hls", "pc", "iptv-all", "captions")
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

        return Multimaps.index(list,
                new Function<Availability, String>() {
                    @Override
                    public String apply(Availability input) {
                        return NitroUtil.programmePid(input);
                    }
                });
    }

    private Callable<ImmutableList<Availability>> exhaustingAvailabilityCallable(final AvailabilityQuery query) {

        return new Callable<ImmutableList<Availability>>() {

            @Override
            public ImmutableList<Availability> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
    }

    private Callable<ImmutableList<Version>> exhaustingVersionsCallable(final VersionsQuery query) {

        return new Callable<ImmutableList<Version>>() {

            @Override
            public ImmutableList<Version> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
    }

    private Callable<ImmutableList<Broadcast>> exhaustingBroadcastsCallable(final BroadcastsQuery query) {

        return new Callable<ImmutableList<Broadcast>>() {

            @Override
            public ImmutableList<Broadcast> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
    }

    private Callable<ImmutableList<Programme>> exhaustingProgrammeCallable(final ProgrammesQuery query) {

        return new Callable<ImmutableList<Programme>>() {

            @Override
            public ImmutableList<Programme> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
    }

    private Iterable<String> toPids(List<Episode> episodes) {
        return Iterables.transform(episodes, new Function<Episode, String>() {
            @Override
            public String apply(Episode input) {
                return input.getPid();
            }
        });
    }

}
