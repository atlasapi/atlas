package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Sets;
import com.google.common.base.Strings;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtChannelDataUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(BtChannelDataUpdater.class);
    private static final String uriFormat = "http://%s/%s";

    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;
    private final String aliasNamespace;
    private final Publisher publisher;

    private BtChannelDataUpdater(Builder builder) {
        channelResolver = checkNotNull(builder.channelResolver);
        channelWriter = checkNotNull(builder.channelWriter);
        aliasNamespace = checkNotNull(builder.aliasNamespace);
        publisher = checkNotNull(builder.publisher);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void addAliasesToChannel(PaginatedEntries paginatedEntries) {

        Set<Long> updatedChannels = Sets.newHashSet();

        for(Entry currentEntry : paginatedEntries.getEntries()) {

            try {
                Optional<Channel> channelOptional = processEntryForAliases(currentEntry);

                channelOptional.ifPresent(channel -> updatedChannels.add(channel.getId()));

            } catch (IllegalArgumentException e) {
                LOGGER.error("Failure to process. Channel Id may contain illegal characters that are not accepted by the codec", e);
            }

        }

        removeStaleAliasesFromChannel(updatedChannels, channelResolver.all());
    }

    public void addAvailableDatesToChannel(PaginatedEntries paginatedEntries) {
        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> updatedChannels = Sets.newHashSet();

        for(Entry currentEntry : entries) {
            try {
                Optional<Channel> channelOptional = processEntryForAdvertisedDates(currentEntry);

                channelOptional.ifPresent(channel -> updatedChannels.add(channel.getId()));

            } catch (IllegalArgumentException e) {
                LOGGER.error("Failure to process. Channel Id may contain illegal characters that are not accepted by the codec", e);
            }

        }

        removeStaleAvailableDateFromChannel(updatedChannels, channelResolver.all());
    }

    private Optional<Channel> processEntryForAliases(Entry entry) {
        String linearEpgChannelId = entry.getLinearEpgChannelId();

        Optional<Channel> channelOptional = channelFor(entry.getGuid());

        if (!channelOptional.isPresent()) {
            return Optional.empty();
        }

        Channel channel = channelOptional.get();
        updateChannelWithAliases(findOrCreateSourceChannel(channel), linearEpgChannelId);

        return updateChannelWithAliases(channel, linearEpgChannelId);
    }

    private Optional<Channel> updateChannelWithAliases(Channel channel, String linearEpgChannelId) {
        channel.setAliases(
                channel.getAliases()
                        .stream()
                        .filter(alias -> !alias.getNamespace().equals(aliasNamespace))
                        .collect(Collectors.toList())
        );

        if (!Strings.isNullOrEmpty(linearEpgChannelId)) {
            channel.addAlias(new Alias(aliasNamespace, linearEpgChannelId));
            channelWriter.createOrUpdate(channel);
            return Optional.of(channel);
        }

        //There can be a channel for a channel id that doesn't have the linearEpgChannelId field.
        return Optional.empty();
    }

    private Optional<Channel> processEntryForAdvertisedDates(Entry entry) {

        DateTime advertiseFromDate = new DateTime(entry.getAvailableDate());
        DateTime advertiseToDate = new DateTime(entry.getAvailableToDate());

        Optional<Channel> channelOptional = channelFor(entry.getGuid());

        if (!channelOptional.isPresent()) {
            return Optional.empty();
        }

        Channel channel = channelOptional.get();
        updateChannelWithAdvertisedDates(findOrCreateSourceChannel(channel), advertiseFromDate, advertiseToDate);

        return Optional.of(updateChannelWithAdvertisedDates(channel, advertiseFromDate, advertiseToDate));

    }

    private Channel updateChannelWithAdvertisedDates(
            Channel channel,
            DateTime advertiseFromDate,
            DateTime advertiseToDate
    ) {
        if (advertiseFromDate != null && advertiseFromDate.getMillis() > 0) {
            channel.setAdvertiseFrom(advertiseFromDate);
        } else {
            channel.setAdvertiseFrom(null);
        }

        if (advertiseToDate != null && advertiseToDate.getMillis() > 0) {
            channel.setAdvertiseTo(advertiseToDate);
        } else {
            channel.setAdvertiseTo(null);
        }

        channelWriter.createOrUpdate(channel);
        return channel;

    }

    private Optional<Channel> channelFor(String guid) {
        long channelId;
        try {
            channelId = codec.decode(guid).longValue();
        } catch(IllegalArgumentException e) {
            LOGGER.error("%s was not valid for decoding", guid, e);
            return Optional.empty();
        }

        Optional<Channel> channelMaybe = channelResolver.fromId(channelId).toOptional();

        if(!channelMaybe.isPresent()) {
            LOGGER.error("There is missing channel for this channel id: %s", guid);
            return Optional.empty();
        }

        return channelMaybe;
    }

    private void removeStaleAliasesFromChannel(
            Set<Long> channelIdsThatHaveAliasesAdded,
            Iterable<Channel> channels
    ) {

        for (Channel channel : channels) {
            if(!channelIdsThatHaveAliasesAdded.contains(channel.getId())) {
                channel.setAliases(
                        channel.getAliases()
                                .stream()
                                .filter(alias -> !alias.getNamespace().equals(aliasNamespace))
                                .collect(Collectors.toList())
                );
            }
            channelWriter.createOrUpdate(channel);
        }
    }

    private void removeStaleAvailableDateFromChannel(
            Set<Long> channelIdsThatHaveAvailableDateAdded,
            Iterable<Channel> channels
    ) {

        for(Channel channel : channels) {
            if(!channelIdsThatHaveAvailableDateAdded.contains(channel.getId())) {
                channel.setAdvertiseFrom(null);
            }
            channelWriter.createOrUpdate(channel);
        }
    }

    private Channel findOrCreateSourceChannel(Channel channel) {

        String baseChannelUri = String.format(
                uriFormat,
                publisher.key(),
                codec.encode(BigInteger.valueOf(channel.getId()))
        );

        return channelResolver.fromUri(baseChannelUri).valueOrDefault(
                Channel.builder()
                        .withUri(baseChannelUri)
                        .withMediaType(MediaType.VIDEO)
                        .withChannelType(ChannelType.CHANNEL)
                        .withSource(publisher)
                        .build()
        );
    }

    public static final class Builder {

        private ChannelResolver channelResolver;
        private ChannelWriter channelWriter;
        private String aliasNamespace;
        private Publisher publisher;

        private Builder() {
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withChannelWriter(ChannelWriter channelWriter) {
            this.channelWriter = channelWriter;
            return this;
        }

        public Builder withAliasNamespace(String aliasNamespace) {
            this.aliasNamespace = aliasNamespace;
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public BtChannelDataUpdater build() {
            return new BtChannelDataUpdater(this);
        }
    }
}
