package org.atlasapi.remotesite.bt.channels;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractBtChannelGroupSaver {

    // deliberately internalising the codec, since there are many ID codecs flying around
    // and these are v4 style, lowercase only ones, used by the external feed. Anything
    // else would not pass muster
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final Function<String, Long> TO_NUMERIC_ID = input -> codec.decode(input).longValue();

    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelGroupWriter channelGroupWriter;
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;
    private final Publisher publisher;
    private final Logger log;

    public AbstractBtChannelGroupSaver(
            Publisher publisher,
            ChannelGroupResolver channelGroupResolver,
            ChannelGroupWriter channelGroupWriter,
            ChannelResolver channelResolver,
            ChannelWriter channelWriter,
            Logger log
    ) {
        this.publisher = checkNotNull(publisher);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.channelGroupWriter = checkNotNull(channelGroupWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.channelWriter = checkNotNull(channelWriter);
        this.log = checkNotNull(log);
    }

    protected void start() { /* do nothing by default */ }

    protected abstract List<String> keysFor(Entry channel);
    protected abstract Set<Alias> aliasesFor(String key);
    protected abstract String aliasUriFor(String key);
    protected abstract String titleFor(String key);

    public Set<String> update(Iterable<Entry> channels) {
        start();
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        for (Entry channel : channels) {
            List<String> keys = keysFor(channel);

            for (String key : keys) {
                for (String mbid : channel.getGuid()) {
                    builder.put(key, mbid);
                }
            }
        }
        return updateChannelGroups(builder.build());
    }

    private Set<String> updateChannelGroups(ImmutableMultimap<String, String> keys) {
        ImmutableSet.Builder<String> channelGroupUris = ImmutableSet.builder();
        for (Map.Entry<String, Collection<String>> entry : keys.asMap().entrySet()) {
            String aliasUri = aliasUriFor(entry.getKey());
            Set<Alias> aliases = aliasesFor(entry.getKey());

            ChannelGroup channelGroup = getOrCreateChannelGroup(aliasUri, aliases);
            channelGroup.setPublisher(publisher);
            channelGroup.addTitle(titleFor(entry.getKey()));

            // update the listed channels with this channel group
            Set<Long> channels;
            try {
                channels = updateChannelNumberingInChannels(entry, channelGroup);
            } catch (Exception e) {
                log.error("Failure to process. Channel Id may contain illegal characters that cannot be decoded", e);
                channels = ImmutableSet.of();
            }
            Set<Long> currentChannels = ImmutableSet.copyOf(channels);

            // remove the channel group from any that were listed, and now aren't
            channelGroup.getChannelNumberings().stream()
                    .map(ChannelNumbering::getChannel)
                    .filter(id -> !currentChannels.contains(id))
                    .forEach(id -> removeChannelGroupFromChannel(id, channelGroup));

            // update the channels on the channelGroup
            setCurrentChannelsToChannelGroup(channelGroup, currentChannels);
            channelGroupWriter.createOrUpdate(channelGroup);

            channelGroupUris.add(channelGroup.getCanonicalUri());
        }
        return channelGroupUris.build();
    }

    private Set<Long> updateChannelNumberingInChannels(
            Map.Entry<String, Collection<String>> entry,
            ChannelGroup channelGroup
    ) {
        Set<Long> currentChannels = Sets.newHashSet();
        for (String channelId : entry.getValue()) {
            try {
                Long numericId = TO_NUMERIC_ID.apply(channelId);
                currentChannels.add(numericId);
                Channel channel = Iterables.getOnlyElement(
                        channelResolver.forIds(ImmutableSet.of(numericId)),
                        null
                );
                if (channel != null) {
                    channel.addChannelNumber(ChannelNumbering.builder()
                            .withChannelGroup(channelGroup)
                            .build());
                    channelWriter.createOrUpdate(channel);
                } else {
                    log.warn("Could not resolve channel with ID {}", channelId);
                }
            } catch (Exception e) {
                log.error(
                        "Error processing channel id {} with channel group {}",
                        channelId,
                        channelGroup.getId(),
                        e
                );
            }
        }

        return currentChannels;
    }

    /**
     * Remove the channel group from the numberings of the channel.
     * @param channelId     the id of the channel to modify.
     * @param channelGroup  the channel group to remove.
     * @return  {@code false} if it wasn't there in the first place.
     */
    private boolean removeChannelGroupFromChannel(
            Long channelId,
            ChannelGroup channelGroup
    ) {
        Channel channel = Iterables.getOnlyElement(
                channelResolver.forIds(ImmutableSet.of(channelId)),
                null
        );
        if (channel == null) {
            log.warn("Could not resolve channel with ID {}", channelId);
            return false;
        }
        if (channel.getChannelNumbers() == null) {
            return false;   // no numberings
        }
        ImmutableSet.Builder<ChannelNumbering> newNumberings = ImmutableSet.builder();
        boolean modified = false;
        for (ChannelNumbering channelNumbering : channel.getChannelNumbers()) {
            if (channelGroup.getId().equals(channelNumbering.getChannelGroup())) {
                // don't add it to the new list
                modified = true;    // and we'll need to do the write
            } else {
                newNumberings.add(channelNumbering);
            }
        }
        if (!modified) {
            // all channelNumberings have been added to the new list,
            // so there's no change, so no point doing anything.
            return false;
        }
        channel.setChannelNumbers(newNumberings.build());
        channelWriter.createOrUpdate(channel);
        return true;
    }

    private void setCurrentChannelsToChannelGroup(
            final ChannelGroup channelGroup,
            Set<Long> currentChannels
    ) {
        ImmutableList.Builder<ChannelNumbering> channelNumberings = ImmutableList.builder();
        for (Long channelId : currentChannels) {
            ChannelNumbering channel = ChannelNumbering.builder()
                    .withChannel(channelId)
                    .withChannelGroup(channelGroup)
                    .build();
            channelNumberings.add(channel);
        }
        channelGroup.setChannelNumberings(channelNumberings.build());
    }

    private ChannelGroup getOrCreateChannelGroup(String uri, Set<Alias> aliases) {
        Optional<ChannelGroup> channelGroup = channelGroupResolver.channelGroupFor(uri);

        ChannelGroup group;
        if (channelGroup.isPresent()) {
            group = channelGroup.get();
        } else {
            group = new Region();
            group.setCanonicalUri(uri);
            // Adding channels to channel groups requires that the channel group has
            // and ID, so we save now.
            channelGroupWriter.createOrUpdate(group);
        }

        group.setAliases(aliases);

        return group;
    }

}
