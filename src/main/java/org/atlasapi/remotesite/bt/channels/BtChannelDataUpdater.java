package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Sets;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.not;
import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

public class BtChannelDataUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(BtChannelDataUpdater.class);

    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec().lowerCaseOnly();
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;
    private final String aliasNamespace;

    public BtChannelDataUpdater(ChannelResolver channelResolver, ChannelWriter channelWriter,
                                String aliasNamespace) {
        this.channelResolver = checkNotNull(channelResolver);
        this.channelWriter = checkNotNull(channelWriter);
        this.aliasNamespace = checkNotNull(aliasNamespace);
    }

    public void addAliasesToChannel(PaginatedEntries paginatedEntries) {

        Set<Long> updatedChannels = Sets.newHashSet();

        for(Entry currentEntry : paginatedEntries.getEntries()) {

            try {
                Maybe<Channel> channel = processEntryForAliases(currentEntry);
                if(channel.hasValue()) {
                    updatedChannels.add(channel.requireValue().getId());
                }

            } catch (Exception e) {
                LOGGER.error("Failure to process rows...", e);
            }

        }

        removeStaleAliasesFromChannel(updatedChannels, channelResolver.all());
    }

    public void addAvailableDateToChannel(PaginatedEntries paginatedEntries) {
        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> updatedChannels = Sets.newHashSet();

        for(Entry currentEntry : entries) {
            try {
                Maybe<Channel> channel = processEntryForAdvertiseFrom(currentEntry);
                if (channel.hasValue()) {
                    updatedChannels.add(channel.requireValue().getId());
                }
            } catch (Exception e) {
                LOGGER.error("Failed to process row....", e);
            }

        }

        removeStaleAvailableDateFromChannel(updatedChannels, channelResolver.all());
    }

    private Maybe<Channel> processEntryForAliases(Entry entry) {
        String linearEpgChannelId = entry.getLinearEpgChannelId();

        Maybe<Channel> channelMaybe = channelFor(entry.getGuid());

        Channel channel = channelMaybe.requireValue();

        channel.setAliases(
                Iterables.filter(channel.getAliases(),
                        not(isAliasWithNamespace(aliasNamespace)))
        );

        if (!Strings.isNullOrEmpty(linearEpgChannelId)) {
            channel.addAlias(new Alias(aliasNamespace, linearEpgChannelId));
            channelWriter.createOrUpdate(channel);
            return Maybe.just(channel);
        }

        //There can be a channel for a channel id that doesn't have the linearEpgChannelId field.
        return Maybe.nothing();
    }

    private Maybe<Channel> processEntryForAdvertiseFrom(Entry entry) {
        long availableDate = entry.getAvailableDate();
        DateTime advertiseAvailableDate = new DateTime(entry.getAvailableDate());

        Maybe<Channel> channelMaybe = channelFor(entry.getGuid());

        Channel channel = channelMaybe.requireValue();

        if (advertiseAvailableDate != null && advertiseAvailableDate.getMillis() > 0) {
            channel.setAdvertiseFrom(advertiseAvailableDate);
        } else {
            channel.setAdvertiseFrom(null);
        }

        channelWriter.createOrUpdate(channel);
        return Maybe.just(channel);
    }

    private Maybe<Channel> channelFor(String guid) {
        long channelId = codec.decode(guid).longValue();

        Maybe<Channel> channelMaybe = channelResolver.fromId(channelId);

        if(!channelMaybe.hasValue()) {
            LOGGER.error("There is missing channel for this channel id: " + guid);
            return Maybe.nothing();
        }
        return channelMaybe;
    }

    private void removeStaleAliasesFromChannel(Set<Long> channelIdsThatHaveAliasesAdded, Iterable<Channel> channels) {

        for(Channel channel : channels) {
            if(!channelIdsThatHaveAliasesAdded.contains(channel.getId())) {
                channel.setAliases(
                        Iterables.filter(channel.getAliases(),
                                not(isAliasWithNamespace(aliasNamespace)))
                );
            }
            channelWriter.createOrUpdate(channel);
        }
    }

    private void removeStaleAvailableDateFromChannel(Set<Long> channelIdsThatHaveAvailableDateAdded, Iterable<Channel> channels) {

        for(Channel channel : channels) {
            if(!channelIdsThatHaveAvailableDateAdded.contains(channel.getId())) {
                channel.setAdvertiseFrom(null);
            }
            channelWriter.createOrUpdate(channel);
        }
    }

    private Predicate<Alias> isAliasWithNamespace(final String namespace) {

        return new Predicate<Alias>() {

            @Override
            public boolean apply(Alias alias) {
                return alias.getNamespace().equals(namespace);
            }
        };
    }
}
