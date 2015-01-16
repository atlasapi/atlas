package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;

import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBrandExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroSeriesExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroClient;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroGenreGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroContentAdapter.class);
    
    private final Glycerin glycerin;
    private final GlycerinNitroClipsAdapter clipsAdapter;
    private final NitroClient nitroClient;

    private final NitroBrandExtractor brandExtractor;
    private final NitroSeriesExtractor seriesExtractor;
    private final NitroEpisodeExtractor itemExtractor;
    private final int pageSize;
    
    public GlycerinNitroContentAdapter(Glycerin glycerin, NitroClient nitroClient, Clock clock, int pageSize) {
        this.glycerin = checkNotNull(glycerin);
        this.nitroClient = checkNotNull(nitroClient);
        this.pageSize = pageSize;
        this.clipsAdapter = new GlycerinNitroClipsAdapter(glycerin, clock, pageSize);
        this.brandExtractor = new NitroBrandExtractor(clock);
        this.seriesExtractor = new NitroSeriesExtractor(clock);
        this.itemExtractor = new NitroEpisodeExtractor(clock);
    }
    
    @Override
    public ImmutableSet<Brand> fetchBrands(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "brand");
            ImmutableList<Programme> programmes = fetchProgrammes(refs);
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
            ImmutableList<Programme> programmes = fetchProgrammes(refs);
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

    @Override
    public ImmutableSet<Item> fetchEpisodes(Iterable<PidReference> refs) throws NitroException {
        if (Iterables.isEmpty(refs)) {
            return ImmutableSet.of();
        }
        try {
            checkRefType(refs, "episode");
            ImmutableList<Programme> programmes = fetchProgrammes(refs);
            
            if (programmes.isEmpty()) {
                log.warn("No programmes found for refs {}", Iterables.transform(refs, new Function<PidReference, String>() {

                    @Override
                    public String apply(PidReference pidRef) {
                        return pidRef.getPid();
                    }
                    
                }));
                return ImmutableSet.of();
            }
            
            ImmutableList<Episode> episodes = getAsEpisodes(programmes);
            ImmutableList<NitroItemSource<Episode>> sources = toItemSources(episodes);
            Multimap<String, Clip> clips = clipsAdapter.clipsFor(refs);
            ImmutableSet.Builder<Item> fetched = ImmutableSet.builder();
            for (NitroItemSource<Episode> source : sources) {
                Item item = itemExtractor.extract(source);
                item.setClips(clips.get(item.getCanonicalUri()));
                fetched.add(item);
            }
            return fetched.build();
        } catch (GlycerinException e) {
            throw new NitroException(NitroUtil.toPids(refs).toString(), e);
        }
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

    private ImmutableList<Programme> fetchProgrammes(Iterable<PidReference> refs) throws GlycerinException {
        ProgrammesQuery query = ProgrammesQuery.builder()
                .withPid(toStrings(refs))
                .withMixins(TITLES)
                .withPageSize(pageSize)
                .build();
        return exhaust(glycerin.execute(query));
    }

    private <T> ImmutableList<T> exhaust(GlycerinResponse<T> resp) throws GlycerinException {
        ImmutableList.Builder<T> programmes = ImmutableList.builder(); 
        programmes.addAll(resp.getResults());
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
        BroadcastsQuery query = BroadcastsQuery.builder()
                .withDescendantsOf(toPids(episodes))
                .withPageSize(pageSize)
                .build();
        return Multimaps.index(exhaust(glycerin.execute(query)), new Function<Broadcast, String>() {
            @Override
            public String apply(Broadcast input) {
                return NitroUtil.programmePid(input).getPid();
            }
        });
    }

    private ListMultimap<String, Version> versions(ImmutableList<Episode> episodes) throws GlycerinException {
        VersionsQuery query = VersionsQuery.builder()
                .withDescendantsOf(toPids(episodes))
                .withPageSize(pageSize)
                .build();
        return Multimaps.index(exhaust(glycerin.execute(query)), new Function<Version, String>() {
            @Override
            public String apply(Version input) {
                return NitroUtil.programmePid(input).getPid();
            }
        });
    }

    private ListMultimap<String, Availability> availabilities(ImmutableList<Episode> episodes) throws GlycerinException {
        if (episodes.isEmpty()) {
            return ImmutableListMultimap.of();
        }
        
        AvailabilityQuery query = AvailabilityQuery.builder()
                .withDescendantsOf(toPids(episodes))
                .withPageSize(pageSize)
                .withMediaSet("apple-iphone4-ipad-hls-3g", "apple-iphone4-hls", "pc", "iptv-all")
                .build();
        return Multimaps.index(exhaust(glycerin.execute(query)),
                new Function<Availability, String>() {
                    @Override
                    public String apply(Availability input) {
                        return NitroUtil.programmePid(input);
                    }
                });
    }

    private Iterable<String> toPids(ImmutableList<Episode> episodes) {
        return Iterables.transform(episodes, new Function<Episode, String>() {
            @Override
            public String apply(Episode input) {
                return input.getPid();
            }
        });
    }
    
}
