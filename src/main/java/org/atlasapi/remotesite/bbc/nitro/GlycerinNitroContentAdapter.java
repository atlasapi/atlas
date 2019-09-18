package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBrandExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroSeriesExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.ANCESTOR_TITLES;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.AVAILABLE_VERSIONS;
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
        public Episode apply(Programme input) {
            return input.getAsEpisode();
        }
    };
    private static final Function<List<Programme>, List<Episode>> TO_EPISODES_LIST = new Function<List<Programme>, List<Episode>>() {

        @Override
        public List<Episode> apply(List<Programme> input) {
            return ImmutableList.copyOf(Iterables.transform(input, toEpisode()));
        }
    };
    private static final Predicate<Programme> IS_EPISODE = new Predicate<Programme>() {

        @Override
        public boolean apply(Programme input) {
            return input.isEpisode();
        }
    };
    private static final Function<List<Programme>, List<Programme>> FILTER_EPISODES = new Function<List<Programme>, List<Programme>>() {

        @Override
        public List<Programme> apply(List<Programme> input) {
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

    public static final Function<List<Programme>, List<Programme>> filterEpisodes() {
        return FILTER_EPISODES;
    }


    @Override
    public ImmutableSet<ModelWithPayload<Brand>> fetchBrands(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "brand");
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(
                    makeProgrammeQueries(refs)
            );
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<ModelWithPayload<Brand>> fetched = ImmutableSet.builder();
            for (List<Programme> programmes : currentProgrammes) {
                for (Programme programme : programmes) {
                    if (programme.isBrand()) {
                        Brand brand = brandExtractor.extract(programme.getAsBrand());
                        brand.setClips(clips.get(brand.getCanonicalUri()));
                        ModelWithPayload brandWithPayload = new ModelWithPayload(brand, programme);
                        fetched.add(brandWithPayload);
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
    public ImmutableSet<ModelWithPayload<Series>> fetchSeries(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "series");
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(
                    makeProgrammeQueries(refs)
            );
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<ModelWithPayload<Series>> fetched = ImmutableSet.builder();
            for (List<Programme> programmes : currentProgrammes) {
                for (Programme programme : programmes) {
                    if (programme.isSeries()) {
                        Series series = seriesExtractor.extract(programme.getAsSeries());
                        series.setClips(clips.get(series.getCanonicalUri()));
                        ModelWithPayload<Series> seriesWithPayload = new ModelWithPayload<>(series, programme);
                        fetched.add(seriesWithPayload);
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
                    .withMixins(
                            ANCESTOR_TITLES,
                            CONTRIBUTIONS,
                            IMAGES,
                            GENRE_GROUPINGS,
                            AVAILABLE_VERSIONS
                    )
                    .withPageSize(pageSize)
                    .build();

            queries.add(query);
        }

        return queries.build();
    }

    @Override
    public Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(
            ProgrammesQuery programmesQuery,
            ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws NitroException {

        try {
            Iterable<List<Programme>> programmes = fetchProgrammes(ImmutableList.of(programmesQuery));
            return fetchEpisodesFromProgrammes(
                    programmes,
                    broadcasts
            );
        } catch (GlycerinException e) {
            throw new NitroException(programmesQuery.toString(), e);
        }
    }

    @Override
    public Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(Iterable<PidReference> refs)
            throws NitroException {
        try {
            Iterable<ProgrammesQuery> programmesQueries = makeProgrammeQueries(refs);
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(programmesQueries);
            return fetchEpisodesFromProgrammes(currentProgrammes, null);
        } catch (GlycerinException e) {
            throw new NitroException(refs.toString(), e);
        }
    }

    @Override
    public Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(
            Iterable<PidReference> refs,
            ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws NitroException {
        try {
            Iterable<ProgrammesQuery> programmesQueries = makeProgrammeQueries(refs);
            Iterable<List<Programme>> currentProgrammes = fetchProgrammes(programmesQueries);
            return fetchEpisodesFromProgrammes(currentProgrammes, broadcasts);
        } catch (GlycerinException e) {
            throw new NitroException(refs.toString(), e);
        }
    }

    private Iterable<List<ModelWithPayload<Item>>> fetchEpisodesFromProgrammes(
            Iterable<List<Programme>> currentProgrammes,
            @Nullable ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws NitroException, GlycerinException {
        Iterable<List<Episode>> programmesAsEpisodes = getAsEpisodes(currentProgrammes);
        return toItemsListIterable(programmesAsEpisodes, broadcasts);
    }

    private Iterable<List<ModelWithPayload<Item>>> toItemsListIterable(
            Iterable<List<Episode>> episodes,
            @Nullable ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws GlycerinException, NitroException {
        return new PaginatedNitroItemSources(
                episodes,
                executor,
                glycerin,
                pageSize,
                itemExtractor,
                clipsAdapter,
                broadcasts
        );
    }

    private Iterable<List<Episode>> getAsEpisodes(Iterable<List<Programme>> programmes) {
        Iterable<List<Programme>> episodes = Iterables.transform(
                programmes,
                filterEpisodes()
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
