package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

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

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.time.Clock;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Function<Programme, Episode> TO_EPISODE = new Function<Programme, Episode>() {

        @Override
        public Episode apply(@Nullable Programme input) {
            return input.getAsEpisode();
        }
    };
    public static final Predicate<Programme> IS_EPISODE = new Predicate<Programme>() {

        @Override
        public boolean apply(Programme input) {
            return input.isEpisode();
        }
    };

    private final Glycerin glycerin;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    private final NitroBrandExtractor brandExtractor;
    private final NitroSeriesExtractor seriesExtractor;
    private final NitroEpisodeExtractor itemExtractor;
    private final int pageSize;

    private final ListeningExecutorService executor;

    public GlycerinNitroContentAdapter(
            Glycerin glycerin,
            GlycerinNitroClipsAdapter clipsAdapter,
            QueuingPersonWriter peopleWriter,
            Clock clock,
            int pageSize
    ) {
        this.glycerin = checkNotNull(glycerin);
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
            Iterable<Programme> programmes = fetchProgrammes(makeProgrammeQueries(refs));
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
            Iterable<Programme> programmes = fetchProgrammes(makeProgrammeQueries(refs));
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
    public Iterable<Item> fetchEpisodes(ProgrammesQuery programmesQuery) throws NitroException {
        try {
            Iterable<Programme> programmes = fetchProgrammes(ImmutableList.of(programmesQuery));
            return fetchEpisodesFromProgrammes(programmes);
        } catch (GlycerinException e) {
            throw new NitroException(programmesQuery.toString(), e);
        }
    }

    @Override
    public Iterable<Item> fetchEpisodes(Iterable<PidReference> refs)
            throws NitroException {
        try {
            Iterable<ProgrammesQuery> programmesQueries = makeProgrammeQueries(refs);
            Iterable<Programme> programmes = fetchProgrammes(programmesQueries);
            return fetchEpisodesFromProgrammes(programmes);
        } catch (GlycerinException e) {
            throw new NitroException(refs.toString(), e);
        }
    }

    private Iterable<Item> fetchEpisodesFromProgrammes(Iterable<Programme> programmes)
            throws NitroException, GlycerinException {
        Iterable<Episode> episodes = getAsEpisodes(programmes);
        Iterable<NitroItemSource<Episode>> sources = toItemSources(episodes);

        return new LazyNitroEpisodeExtractor(sources, itemExtractor, clipsAdapter);
    }

    private Iterable<NitroItemSource<Episode>> toItemSources(Iterable<Episode> episodes)
            throws GlycerinException, NitroException {
        return new PaginatedNitroItemSources(
                episodes,
                pageSize,
                executor,
                glycerin
        );
    }

    private Iterable<Episode> getAsEpisodes(Iterable<Programme> programmes) {
        return Iterables.transform(Iterables.filter(programmes, IS_EPISODE), TO_EPISODE);
    }

    private Iterable<Programme> fetchProgrammes(Iterable<ProgrammesQuery> queries)
            throws GlycerinException {

        return new PaginatedProgrammeRequest(glycerin, queries);
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
        return Iterables.transform(refs, new Function<PidReference, String>() {

            @Override
            public String apply(PidReference input) {
                return input.getPid();
            }
        });
    }

    private Callable<ImmutableList<Availability>> exhaustingAvailabilityCallable(
            final AvailabilityQuery query) {

        return new Callable<ImmutableList<Availability>>() {

            @Override
            public ImmutableList<Availability> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
    }

}
