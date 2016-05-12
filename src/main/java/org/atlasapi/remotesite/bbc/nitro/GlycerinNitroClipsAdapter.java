package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.IMAGES;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroClipExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
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
import com.metabroadcast.atlas.glycerin.model.Clip;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;
import com.metabroadcast.common.time.Clock;

/**
 * Adapter to fetch and extract {@link org.atlasapi.media.entity.Clip Clip}s for
 * a {@link PidReference} from Nitro using {@link Glycerin}.
 */
public class GlycerinNitroClipsAdapter {

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroClipsAdapter.class);
    
    private static final int BATCH_SIZE = 100;
    
    private static final Predicate<Programme> isClip
        = new Predicate<Programme>() {
            @Override
            public boolean apply(Programme input) {
                return input.isClip();
            }
        };
    private static final Function<Programme, com.metabroadcast.atlas.glycerin.model.Clip> toClip
        = new Function<Programme, com.metabroadcast.atlas.glycerin.model.Clip>() {
            @Override
            public com.metabroadcast.atlas.glycerin.model.Clip apply(Programme input) {
                return input.getAsClip();
            }
        };

    private final Glycerin glycerin;
    private final NitroClipExtractor clipExtractor;
    private final int pageSize;

    private final ListeningExecutorService executor;

    public GlycerinNitroClipsAdapter(Glycerin glycerin, Clock clock, int pageSize) {
        this.glycerin = glycerin;
        this.clipExtractor = new NitroClipExtractor(clock);
        this.pageSize = pageSize;
        this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(15));
    }
    
    public Multimap<String, org.atlasapi.media.entity.Clip> clipsFor(Iterable<PidReference> refs) throws NitroException {
        try {
            if (Iterables.isEmpty(refs)) {
                return ImmutableMultimap.of();
            }
            
            Iterable<com.metabroadcast.atlas.glycerin.model.Clip> nitroClips
                = Iterables.transform(Iterables.filter(getNitroClips(refs), isClip), toClip);
            
            if (Iterables.isEmpty(nitroClips)) {
                log.warn("No programmes found for clipRefs {}", Iterables.transform(refs, new Function<PidReference, String>() {

                    @Override
                    public String apply(@Nullable PidReference pidRef) {
                        return pidRef.getPid();
                    }
                    
                }));
                return ImmutableMultimap.of();
            }
            
            Iterable<List<Clip>> clipParts = Iterables.partition(nitroClips, BATCH_SIZE);
            ImmutableListMultimap.Builder<String, org.atlasapi.media.entity.Clip> clips
                = ImmutableListMultimap.builder();
            for (List<Clip> clipPart : clipParts) {
                clips.putAll(extractClips(clipPart));
            }
            return clips.build();
        } catch (GlycerinException e) {
            throw new NitroException(NitroUtil.toPids(refs).toString(), e);
        }
    }

    public List<org.atlasapi.media.entity.Clip> clipsFor(PidReference ref) throws NitroException {
        try {
            Iterable<com.metabroadcast.atlas.glycerin.model.Clip> nitroClips
                    = Iterables.transform(Iterables.filter(getNitroClip(ref), isClip), toClip);

            if (Iterables.isEmpty(nitroClips)) {
                log.warn("No programmes found for clipRefs {}", ref, new Function<PidReference, String>() {

                    @Override
                    public String apply(@Nullable PidReference pidRef) {
                        return pidRef.getPid();
                    }
                });
            }

            Iterable<List<Clip>> clipParts = Iterables.partition(nitroClips, BATCH_SIZE);
            ImmutableList.Builder<org.atlasapi.media.entity.Clip> clips = ImmutableList.builder();
            for (List<Clip> clipPart : clipParts) {
                clips.addAll(extractClips(clipPart).values());
            }
            return clips.build();
        } catch (GlycerinException e) {
            throw new NitroException(ref.toString(), e);
        }
    }

    private Multimap<String, org.atlasapi.media.entity.Clip> extractClips(List<Clip> clipPart) throws GlycerinException {
        final ListMultimap<String, Availability> availabilities = getNitroAvailabilities(clipPart);
        final ListMultimap<String, Version> versions = versions(clipPart);
        ImmutableListMultimap.Builder<String, org.atlasapi.media.entity.Clip> extracted
            = ImmutableListMultimap.builder();
        for (Clip clip : clipPart) {
            List<Availability> clipAvailabilities = availabilities.get(clip.getPid());
            
            NitroItemSource<Clip> source = NitroItemSource.valueOf(clip, clipAvailabilities,
                    ImmutableList.<Broadcast>of(), versions.get(clip.getPid()));
            extracted.put(BbcFeeds.nitroUriForPid(clip.getClipOf().getPid()), clipExtractor.extract(source));
        }
        return extracted.build();
    }

    private ListMultimap<String, Version> versions(List<Clip> clips) throws GlycerinException {
        VersionsQuery query = VersionsQuery.builder()
                .withDescendantsOf(toPids(clips))
                .withPageSize(pageSize)
                .build();
        return Multimaps.index(exhaust(glycerin.execute(query)), new Function<Version, String>() {
            @Override
            public String apply(Version input) {
                return NitroUtil.programmePid(input).getPid();
            }
        });
    }
    
    private Iterable<String> toPids(List<Clip> clips) {
        return Iterables.transform(clips, new Function<Clip, String>() {
            @Override
            public String apply(Clip input) {
                return input.getPid();
            }
        });
    }
    
    private ListMultimap<String, Availability> getNitroAvailabilities(List<Clip> clipPart) throws GlycerinException {
        if (clipPart.isEmpty()) {
            return ImmutableListMultimap.of();
        }
        
        AvailabilityQuery query = AvailabilityQuery.builder()
                .withDescendantsOf(toPid(clipPart))
                .withPageSize(pageSize)
                .build();
        GlycerinResponse<Availability> availabilities = glycerin.execute(query);
        return Multimaps.index(availabilities.getResults(), new Function<Availability, String>() {
            @Override
            public String apply(Availability input) {
                return checkNotNull(NitroUtil.programmePid(input));
            }
        });
    }
    
    private Iterable<String> toPid(List<Clip> clipPart) {
        return Lists.transform(clipPart, new Function<Clip, String>() {
            @Override
            public String apply(Clip input) {
                return input.getPid();
            }
        });
    }
    
    private ImmutableList<Programme> getNitroClips(Iterable<PidReference> refs) throws GlycerinException {
        
        List<ListenableFuture<ImmutableList<Programme>>> futures = Lists.newArrayList();
        
        for (List<PidReference> ref : Iterables.partition(refs, 5)) {
            ProgrammesQuery query = ProgrammesQuery.builder()
                    .withEntityType(EntityTypeOption.CLIP)
                    .withChildrenOf(NitroUtil.toPids(ref))
                    .withMixins(IMAGES)
                    .withPageSize(pageSize)
                    .build();
            
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

    private ImmutableList<Programme> getNitroClip(PidReference ref) throws GlycerinException {

        ProgrammesQuery query = ProgrammesQuery.builder()
                .withEntityType(EntityTypeOption.CLIP)
                .withChildrenOf(ref.toString())
                .withMixins(IMAGES)
                .withPageSize(pageSize)
                .build();

        ListenableFuture<ImmutableList<Programme>> future = executor.submit(
                exhaustingProgrammeCallable(query));

        try {
            return ImmutableList.copyOf(future.get());
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof GlycerinException) {
                throw (GlycerinException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }
    
    private Callable<ImmutableList<Programme>> exhaustingProgrammeCallable(final ProgrammesQuery query) {
        
        return new Callable<ImmutableList<Programme>>() {

            @Override
            public ImmutableList<Programme> call() throws Exception {
                return exhaust(glycerin.execute(query));
            }
        };
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

    
}
