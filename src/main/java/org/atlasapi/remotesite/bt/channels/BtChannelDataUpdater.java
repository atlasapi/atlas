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

            String linearEpgChannelId = currentEntry.getLinearEpgChannelId();
            long channelId = codec.decode(currentEntry.getGuid()).longValue();

            Maybe<Channel> channelMaybe = channelResolver.fromId(channelId);

            if (!channelMaybe.hasValue()) {
                LOGGER.error("There is missing channel for this channel id: " + currentEntry.getGuid());
                continue;
            }

            Channel channel = channelMaybe.requireValue();

            channel.setAliases(
                    Iterables.filter(channel.getAliases(),
                            not(isAliasWithNamespace(aliasNamespace)))
            );

            if (!Strings.isNullOrEmpty(linearEpgChannelId)) {
                LOGGER.error("Aliases is added");
                LOGGER.info("Aliases is added.");
                channel.addAlias(new Alias(aliasNamespace, linearEpgChannelId));
                channelWriter.createOrUpdate(channel);
                updatedChannels.add(channelId);
            }
        }

        removeStaleAliasesFromChannel(updatedChannels, channelResolver.all());
    }

    public void addAvailableDateToChannel(PaginatedEntries paginatedEntries) {
        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> channelIdsThatHaveAvailableDateAdded = Sets.newHashSet();

        for(Entry currentEntry : entries) {

            DateTime advertiseAvailableDate = currentEntry.getAvailableDate();
            long channelId = codec.decode(currentEntry.getGuid()).longValue();

            Maybe<Channel> channelMaybe = channelResolver.fromId(channelId);

            if(!channelMaybe.hasValue()) {
                LOGGER.error("There is missing channel for this channel id: " + currentEntry.getGuid());
                continue;
            }

            Channel channel = channelMaybe.requireValue();

            if (advertiseAvailableDate != null && advertiseAvailableDate.getMillis() > 0) {
                LOGGER.info("availableDate is added.");
                LOGGER.error("availableDate is added.");
                channel.setAdvertiseFrom(advertiseAvailableDate);
            } else {
                channel.setAdvertiseFrom(null);
            }

            channelWriter.createOrUpdate(channel);
            channelIdsThatHaveAvailableDateAdded.add(channelId);
        }

        removeStaleAvailableDateFromChannel(channelIdsThatHaveAvailableDateAdded, channelResolver.all());
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
