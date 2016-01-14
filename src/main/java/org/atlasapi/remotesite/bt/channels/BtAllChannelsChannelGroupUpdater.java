package org.atlasapi.remotesite.bt.channels;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;


public class BtAllChannelsChannelGroupUpdater {

    private static final Logger log = LoggerFactory.getLogger(BtAllChannelsChannelGroupUpdater.class);
    private static final String ALIAS_SUFFIX = "allchannels";
    
    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelGroupWriter channelGroupWriter;
    private final long freeviewPlatformChannelGroupId;
    private final String uriPrefix;

    private Publisher publisher;

    public BtAllChannelsChannelGroupUpdater(ChannelGroupWriter channelGroupWriter,
            ChannelGroupResolver channelGroupResolver, String freeviewPlatformChannelGroupId,
            String uriPrefix, Publisher publisher) {
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.channelGroupWriter = checkNotNull(channelGroupWriter);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.publisher = checkNotNull(publisher);
        this.freeviewPlatformChannelGroupId = new SubstitutionTableNumberCodec().decode(freeviewPlatformChannelGroupId).longValue();
    }

    public void update() {
        ChannelGroup freeviewPlatform = getChannelGroup(freeviewPlatformChannelGroupId);
        
        Set<Long> allChannels = getAllChannelsFromRegions((Platform) freeviewPlatform);
        Set<Long> btChannels = getChannelGroup(uriPrefix + AllBtChannelsChannelGroupSaver.BT_CHANNELS_URI_SUFFIX).getChannels();
        ChannelGroup allBtChannelsGroup = createOrQueryBtAllChannelsChannelGroup();
        
        Set<Long> newChannels = Sets.union(allChannels, btChannels);
        
        if (!newChannels.equals(allBtChannelsGroup.getChannels())) {
            allBtChannelsGroup.setChannels(newChannels);
            channelGroupWriter.createOrUpdate(allBtChannelsGroup);
        }
    }
    
    private Set<Long> getAllChannelsFromRegions(Platform platform) {
        Set<Long> channels = Sets.newHashSet();
        for (Long regionId : platform.getRegions()) {
            Optional<ChannelGroup> region = channelGroupResolver.channelGroupFor(regionId);
            if (region.isPresent()) {
                channels.addAll(region.get().getChannels());
            } else {
                log.error("Could not find region " + regionId);
            }
        }
        return channels;
    }
    
    private ChannelGroup getChannelGroup(long channelGroupId) {
        Optional<ChannelGroup> group = channelGroupResolver.channelGroupFor(channelGroupId);
        if (!group.isPresent()) {
            throw new IllegalStateException("Could not find channel group " + channelGroupId);
        }
        return group.get();
    }
    
    private ChannelGroup getChannelGroup(String uri) {
        Optional<ChannelGroup> group = channelGroupResolver.channelGroupFor(uri);
        if (!group.isPresent()) {
            throw new IllegalStateException("Could not find channel group " + uri);
        }
        return group.get();
    }

    private ChannelGroup createOrQueryBtAllChannelsChannelGroup() {
        String canonicalUri = uriPrefix + ALIAS_SUFFIX;
        Optional<ChannelGroup> possibleGroup = channelGroupResolver.fromAlias(canonicalUri);
        
        if (possibleGroup.isPresent()) {
            return possibleGroup.get();
        }
        
        Region group = new Region();
        group.setCanonicalUri(canonicalUri);
        group.setAliasUrls(ImmutableSet.of(canonicalUri));
        group.setPublisher(publisher);
        group.addTitle("All BT channels");
        
        channelGroupWriter.createOrUpdate(group);
        return group;
    }

}
