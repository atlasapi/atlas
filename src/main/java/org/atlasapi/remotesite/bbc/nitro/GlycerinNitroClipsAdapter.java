package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroClipExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Clip;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
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

    public GlycerinNitroClipsAdapter(Glycerin glycerin, Clock clock, int pageSize) {
        this.glycerin = glycerin;
        this.clipExtractor = new NitroClipExtractor(clock);
        this.pageSize = pageSize;
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

    private Multimap<String, org.atlasapi.media.entity.Clip> extractClips(List<Clip> clipPart) throws GlycerinException {
        final ListMultimap<String, Availability> availabilities = getNitroAvailabilities(clipPart);
        ImmutableListMultimap.Builder<String, org.atlasapi.media.entity.Clip> extracted
            = ImmutableListMultimap.builder();
        for (Clip clip : clipPart) {
            List<Availability> clipAvailabilities = availabilities.get(clip.getPid());
            NitroItemSource<Clip> source = NitroItemSource.valueOf(clip, clipAvailabilities);
            extracted.put(BbcFeeds.nitroUriForPid(clip.getClipOf().getPid()), clipExtractor.extract(source));
        }
        return extracted.build();
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
        ProgrammesQuery query = ProgrammesQuery.builder()
                .withEntityType(EntityTypeOption.CLIP)
                .withChildrenOf(NitroUtil.toPids(refs))
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

    
}
