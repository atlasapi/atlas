package org.atlasapi.remotesite.pa.channels;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.pa.channels.bindings.Station;
import org.atlasapi.remotesite.pa.channels.bindings.TvChannelData;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

public class PaChannelDataHandler {
    
    private static final String PRESS_ASSOCIATION_URL = "http://pressassociation.com";
    private static final Iterable<String> KNOWN_ALIAS_PREFIXES = Iterables.concat(
            ImmutableSet.of("http://pressassociation.com/"),
            PaChannelsIngester.YOUVIEW_SERVICE_ID_ALIAS_PREFIXES);
    private static final Predicate<String> IS_KNOWN_ALIAS = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            for (String prefix : KNOWN_ALIAS_PREFIXES) {
                if (input.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }
    };
    private static final Function<Region, String> REGION_TO_URI = new Function<Region, String> () {
        @Override
        public String apply(Region input) {
            return input.getCanonicalUri();
        }
    };

    private final PaChannelsIngester channelsIngester;
    private final PaChannelGroupsIngester channelGroupsIngester;
    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelGroupWriter channelGroupWriter;
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;
    private final Map<String, Channel> channelMap = Maps.newHashMap();
    private final ChannelNumberingFilterer numberingFilterer;
    
    public PaChannelDataHandler(PaChannelsIngester channelsIngester, PaChannelGroupsIngester channelGroupsIngester, ChannelResolver channelResolver, ChannelWriter channelWriter, ChannelGroupResolver channelGroupResolver, ChannelGroupWriter channelGroupWriter) {
        this.channelsIngester = channelsIngester;
        this.channelGroupsIngester = channelGroupsIngester;
        this.channelResolver = channelResolver;
        this.channelWriter = channelWriter;
        this.channelGroupResolver = channelGroupResolver;
        this.channelGroupWriter = channelGroupWriter;
        this.numberingFilterer = new ChannelNumberingFilterer(channelGroupResolver);
    }
    
    public void handle(TvChannelData channelData) {
        
        channelMap.clear();
        
        for (Station station : channelData.getStations().getStation()) {
            ChannelTree channelTree = channelsIngester.processStation(station, channelData.getServiceProviders().getServiceProvider());
            Channel parent = channelTree.getParent();
            if (parent != null) {
                parent = createOrMerge(parent);
            }
            for (Channel child : channelTree.getChildren()) {
                if (parent != null) {
                    child.setParent(parent);
                }
                for (String alias : child.getAliasUrls()) {
                    if (isPaAlias(alias)) {
                        channelMap.put(alias, child);
                    }
                }
            }
        }
        
        for (org.atlasapi.remotesite.pa.channels.bindings.Platform paPlatform : channelData.getPlatforms().getPlatform()) {
            ChannelGroupTree channelGroupTree = channelGroupsIngester.processPlatform(paPlatform, channelData.getServiceProviders().getServiceProvider(), channelData.getRegions().getRegion());
            
            Platform platform = (Platform) createOrMerge(channelGroupTree.getPlatform());

            if (channelGroupTree.getRegions().isEmpty()) {
                // non-regionalised platform
                channelGroupsIngester.addChannelsToPlatform(platform, paPlatform.getEpg().getEpgContent(), channelMap);
            } else {
                Map<String, Region> writtenRegions = Maps.newHashMap();
                for (Entry<String, Region> entry : channelGroupTree.getRegions().entrySet()) {
                    Region region = entry.getValue();
                    region.setPlatform(platform);
                    region = (Region) createOrMerge(region);
                    writtenRegions.put(entry.getKey(), region);
                }
                channelGroupsIngester.addChannelNumberings(paPlatform.getEpg().getEpgContent(), writtenRegions, channelMap);
            }
            
            removeExpiredRegionsFromPlatform(
                    ImmutableSet.copyOf(Iterables.transform(
                            channelGroupTree.getRegions().values(), 
                            REGION_TO_URI
                    )), 
                    platform
            );
        }
        
        // write channels 
        // TODO should this be multi-threaded? is slowest part by far...
        for (Channel child : channelMap.values()) {
            createOrMerge(child);
        }
    }

    private void removeExpiredRegionsFromPlatform(Set<String> newRegionUris, Platform platform) {
        Set<Long> regionIds = platform.getRegions();
        Map<String, Long> previousRegionUris = Maps.newHashMap();
        for (Long regionId : regionIds) {
            Optional<ChannelGroup> group = channelGroupResolver.channelGroupFor(regionId);
            previousRegionUris.put(group.get().getCanonicalUri(), regionId);
        }

        Collection<Long> redundantRegionIds = Maps.filterKeys(
                previousRegionUris, 
                Predicates.in(Sets.difference(previousRegionUris.keySet(), newRegionUris))
            ).values();
        
        if (!redundantRegionIds.isEmpty()) {
            platform = (Platform) channelGroupResolver.channelGroupFor(platform.getId()).get();
            Set<Long> regions = Sets.newHashSet(platform.getRegions());
            regions.removeAll(redundantRegionIds);
            platform.setRegionIds(regions);
            channelGroupWriter.createOrUpdate(platform);
            
            for (Long redundantRegionId : redundantRegionIds) {
                Region region = (Region) channelGroupResolver.channelGroupFor(redundantRegionId).get();
                region.setPlatform((Long) null);
                channelGroupWriter.createOrUpdate(region);
            }
        }
    }
    
    private Channel createOrMerge(Channel newChannel) {
        String alias = null;
        for (String newAlias : newChannel.getAliasUrls()) {
            if (isPaAlias(newAlias)) {
                alias = newAlias;
                break;
            }
        }
        checkNotNull(alias, "channel with uri " + newChannel.getCanonicalUri() + " has no aliases");
        
        Maybe<Channel> existing = channelResolver.forAlias(alias);
        if (existing.hasValue()) {
            Channel existingChannel = existing.requireValue();

            existingChannel.setTitles(newChannel.getAllTitles());
            existingChannel.setAdult(newChannel.getAdult());
            existingChannel.setImages(newChannel.getAllImages());
            existingChannel.setStartDate(newChannel.getStartDate());
            existingChannel.setEndDate(newChannel.getEndDate());
            existingChannel.setRelatedLinks(newChannel.getRelatedLinks());
            existingChannel.setAliasUrls(mergeChannelAliasUrls(existingChannel.getAliasUrls(), newChannel.getAliasUrls()));
            existingChannel.setParent(newChannel.getParent());
            existingChannel.setMediaType(newChannel.getMediaType());
            existingChannel.setHighDefinition(newChannel.getHighDefinition());
            existingChannel.setRegional(newChannel.getRegional());
            existingChannel.setTimeshift(newChannel.getTimeshift());
            existingChannel.setGenres(newChannel.getGenres());
            // unions new PA numberings with existing non-PA numberings
            existingChannel.setChannelNumbers(Sets.union(
                    newChannel.getChannelNumbers(), 
                    Sets.newHashSet(numberingFilterer.filterNotEqualToGroupPublisher(existingChannel.getChannelNumbers(), Publisher.METABROADCAST))
            ));
            
            return channelWriter.createOrUpdate(existingChannel);
        } else {
            return channelWriter.createOrUpdate(newChannel);
        }
    }
    
    /**
     * <p>
     * Merges the existing alias urls with the newly ingested aliases, replacing those with known prefixes
     *  with newer versions if appropriate.
     * </p>
     * <p>
     * The channels adapter writes certain aliases on channels explicitly, all others it passes through.
     * It maintains a list of prefixes for those known aliases, and removes those from the existing aliases before
     * adding the newly ingested aliases, thus ensuring that if a known alias is updated, that change is 
     * reflected in the set of aliases written.
     * </p>
     * @param existingAliases - the aliases on any existing channel
     * @param newAliases - the set of newly ingested alias
     * @return - the combined set of aliases to write 
     */
    private Set<String> mergeChannelAliasUrls(Set<String> existingAliases, Set<String> newAliases) {
        Builder<String> combined = ImmutableSet.<String>builder();
               
        combined.addAll(Iterables.filter(existingAliases, Predicates.not(IS_KNOWN_ALIAS)));

        if (Iterables.isEmpty(Iterables.filter(newAliases, IS_KNOWN_ALIAS))) {
            Joiner joinOnComma = Joiner.on(',');
            throw new RuntimeException("One of the aliases ingested (" + joinOnComma.join(newAliases) 
                    + ")does not have a recognised prefix. Known prefixes: " + joinOnComma.join(KNOWN_ALIAS_PREFIXES));
        }
        
        combined.addAll(newAliases);
        
        return combined.build();
    }

    private ChannelGroup createOrMerge(ChannelGroup channelGroup) {
        String alias = null;
        for (String newAlias : channelGroup.getAliasUrls()) {
            if (isPaAlias(newAlias)) {
                alias = newAlias;
                break;
            }
        }
        checkNotNull("channel with uri " + channelGroup.getCanonicalUri() + " has no aliases");
        
        Optional<ChannelGroup> resolved = channelGroupResolver.fromAlias(alias);
        
        if (resolved.isPresent()) {
            ChannelGroup existing = resolved.get();

            existing.addAliasUrls(channelGroup.getAliasUrls());
            existing.setTitles(channelGroup.getAllTitles());
            
            if (channelGroup instanceof Region) {
                if (existing instanceof Region) {
                    ((Region)existing).setPlatform(((Region)channelGroup).getPlatform());
                } else {
                    throw new RuntimeException("new channelGroup with alias " + alias + " and type Region does not match existing channelGroup of type " + existing.getClass());
                }
            }
            
            return channelGroupWriter.createOrUpdate(existing);
        } else {
            return channelGroupWriter.createOrUpdate(channelGroup);
        }
    }

    private boolean isPaAlias(String alias) {
        return (alias.contains(PRESS_ASSOCIATION_URL));
    }
}
