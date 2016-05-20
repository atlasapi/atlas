package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBrandExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroSeriesExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
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

    private final Glycerin glycerin;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    private final NitroBrandExtractor brandExtractor;
    private final NitroSeriesExtractor seriesExtractor;
    private final NitroEpisodeExtractor itemExtractor;
    private final int pageSize;

    private final ListeningExecutorService executor;

    private static final Function<Programme, Episode> TO_EPISODE = new Function<Programme, Episode>() {

        @Override
        public Episode apply(@Nullable Programme input) {
            return input.getAsEpisode();
        }
    };
    private static final Function<List<Programme>, List<Episode>> TO_EPISODES_LIST = new Function<List<Programme>, List<Episode>>() {

        @Nullable
        @Override
        public List<Episode> apply(@Nullable List<Programme> input) {
            return ImmutableList.copyOf(Iterables.transform(input, toEpisode()));
        }
    };
    private static final Predicate<Programme> IS_EPISODE = new Predicate<Programme>() {

        @Override
        public boolean apply(Programme input) {
            return input.isEpisode();
        }
    };
    private static final Function<List<Programme>, List<Programme>> IS_EPISODES_LIST = new Function<List<Programme>, List<Programme>>() {

        @Nullable
        @Override
        public List<Programme> apply(@Nullable List<Programme> input) {
            return ImmutableList.copyOf(Iterables.filter(input, isEpisode()));
        }
    };

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

    public static final Function<Programme, Episode> toEpisode() {
        return TO_EPISODE;
    }

    public static final Function<List<Programme>, List<Episode>> toEpisodesList() {
        return TO_EPISODES_LIST;
    }

    public static final Predicate<Programme> isEpisode() {
        return IS_EPISODE;
    }

    public static final Function<List<Programme>, List<Programme>> isEpisodesList() {
        return IS_EPISODES_LIST;
    }

    @Override
    public ImmutableSet<Brand> fetchBrands(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "brand");
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(makeProgrammeQueries(refs));
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<Brand> fetched = ImmutableSet.builder();
            for (List<Programme> programmes : currentProgrammes) {
                for (Programme programme : programmes) {
                    if (programme.isBrand()) {
                        Brand brand = brandExtractor.extract(programme.getAsBrand());
                        brand.setClips(clips.get(brand.getCanonicalUri()));
                        fetched.add(brand);
                    }
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
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(makeProgrammeQueries(refs));
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<Series> fetched = ImmutableSet.builder();
            for (List<Programme> programmes : currentProgrammes) {
                for (Programme programme : programmes) {
                    if (programme.isSeries()) {
                        Series series = seriesExtractor.extract(programme.getAsSeries());
                        series.setClips(clips.get(series.getCanonicalUri()));
                        fetched.add(series);
                    }
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
            Iterable<List<Programme>> programmes = fetchProgrammes(ImmutableList.of(programmesQuery));
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
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(programmesQueries);
            return fetchEpisodesFromProgrammes(currentProgrammes);
        } catch (GlycerinException e) {
            throw new NitroException(refs.toString(), e);
        }
    }

    private Iterable<Item> fetchEpisodesFromProgrammes(Iterable<List<Programme>> currentProgrammes)
            throws NitroException, GlycerinException {
        Iterable<List<Episode>> programmesAsEpisodes = getAsEpisodes(currentProgrammes);
        return toItems(programmesAsEpisodes);
//
//        return new LazyNitroEpisodeExtractor(sources, itemExtractor, clipsAdapter);
    }

    private Iterable<Item> toItems(Iterable<List<Episode>> episodes)
            throws GlycerinException, NitroException {
        return new PaginatedNitroItemSources(
                episodes,
                executor,
                glycerin,
                pageSize,
                itemExtractor,
                clipsAdapter
        );
    }

    private Iterable<List<Episode>> getAsEpisodes(Iterable<List<Programme>> programmes) {
        Iterable<List<Programme>> episodes = Iterables.transform(
                programmes,
                isEpisodesList()
        );
        return Iterables.transform(
                episodes,
                toEpisodesList()
        );
    }

    private Iterable<List<Programme>> fetchProgrammes(Iterable<ProgrammesQuery> queries)
            throws GlycerinException {

        return new PaginatedProgrammeRequest(glycerin, queries);
    }

    private Iterable<String> toStrings(Iterable<PidReference> refs) {
        return Iterables.transform(refs, new Function<PidReference, String>() {

            @Override
            public String apply(PidReference input) {
                return input.getPid();
            }
        });
    }
}
