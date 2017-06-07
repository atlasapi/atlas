package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtMpxChannelDataIngester extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BtMpxChannelDataIngester.class);
    
    private final BtMpxClient btMpxClient;
    private final List<AbstractBtChannelGroupSaver> channelGroupSavers;
    private final BtAllChannelsChannelGroupUpdater allChannelsGroupUpdater;
    private final ChannelGroupResolver channelGroupResolver;
    private final Publisher publisher;
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;
    private final Lock channelWriterLock;
    private final BtChannelDataUpdater channelDataUpdater;
    private boolean ingestAdvertiseFromField;

    private BtMpxChannelDataIngester(Builder builder) {
        this.btMpxClient = checkNotNull(builder.btMpxClient);
        this.allChannelsGroupUpdater = checkNotNull(builder.allChannelsGroupUpdater);
        this.channelGroupResolver = checkNotNull(builder.channelGroupResolver);
        this.publisher = checkNotNull(builder.publisher);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.channelWriter = checkNotNull(builder.channelWriter);
        this.channelWriterLock = checkNotNull(builder.channelWriterLock);
        this.channelDataUpdater = checkNotNull(builder.channelDataUpdater);
        this.ingestAdvertiseFromField = checkNotNull(builder.ingestAdvertiseFromField);

        ChannelGroupWriter channelGroupWriter = checkNotNull(builder.channelGroupWriter);
        String aliasUriPrefix = checkNotNull(builder.aliasUriPrefix);
        String aliasNamespacePrefix = checkNotNull(builder.aliasNamespacePrefix);

        this.channelGroupSavers = ImmutableList.of(
                new SubscriptionChannelGroupSaver(publisher, aliasUriPrefix, aliasNamespacePrefix,
                        channelGroupResolver, channelGroupWriter, channelResolver, channelWriter),
                new TargetUserGroupChannelGroupSaver(publisher,  aliasUriPrefix, aliasNamespacePrefix,
                        channelGroupResolver, channelGroupWriter, btMpxClient, channelResolver, channelWriter),
                new WatchableChannelGroupSaver(publisher, aliasUriPrefix, aliasNamespacePrefix,
                        channelGroupResolver, channelGroupWriter, channelResolver, channelWriter),
                new OutputProtectionChannelGroupSaver(publisher, aliasUriPrefix, aliasNamespacePrefix,
                        channelGroupResolver, channelGroupWriter, channelResolver, channelWriter),
                new AllBtChannelsChannelGroupSaver(publisher, aliasUriPrefix, aliasNamespacePrefix,
                        channelGroupResolver, channelGroupWriter, channelResolver, channelWriter)
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected void runTask() {
        try {
            log.debug("Acquiring channel writer lock");
            channelWriterLock.lock();
            log.debug("Acquired channel writer lock");
            PaginatedEntries entries = btMpxClient.getChannels(Optional.absent());

            channelDataUpdater.addAliasesToChannel(entries);

            if (ingestAdvertiseFromField) {
                channelDataUpdater.addAvailableDatesToChannel(entries);
            }

            ImmutableSet.Builder<String> allCurrentChannelGroups = ImmutableSet.builder();
            for (AbstractBtChannelGroupSaver saver : channelGroupSavers) {
                allCurrentChannelGroups.addAll(saver.update(entries.getEntries()));
            }
            ImmutableSet<String> allCurrentChannelGroupsBuilt = allCurrentChannelGroups.build();

            removeOldChannelGroupChannels(allCurrentChannelGroupsBuilt);
            allChannelsGroupUpdater.update();
        } catch (BtMpxClientException e) {
            throw Throwables.propagate(e);
        } finally {
            channelWriterLock.unlock();
        }
    }

    private void removeOldChannelGroupChannels(Set<String> allCurrentChannelGroups) {
        
        Set<Long> channelGroupIdsToRemove = Sets.newHashSet();
        for (ChannelGroup channelGroup : channelGroupResolver.channelGroups()) {
            if (publisher.equals(channelGroup.getPublisher())
                    && !allCurrentChannelGroups.contains(channelGroup.getCanonicalUri())) {
                channelGroupIdsToRemove.add(channelGroup.getId());
            }
        }
        
        Predicate<ChannelNumbering> shouldKeepPredicate
            = shouldKeepChannelGroupMembership(channelGroupIdsToRemove);
        
        for (Channel channel : channelResolver.all()) {
            int existingSize = channel.getChannelNumbers().size();
            Iterable<ChannelNumbering> filtered = channel.getChannelNumbers().stream()
                    .filter(shouldKeepPredicate)
                    .collect(Collectors.toList());
            if (Iterables.size(filtered) != existingSize) {
                channel.setChannelNumbers(filtered);
                channelWriter.createOrUpdate(channel);
            }
        }
    }
    
    private Predicate<ChannelNumbering> shouldKeepChannelGroupMembership(final Set<Long> groupsToRemove) {
        return input -> !groupsToRemove.contains(input.getChannelGroup());
    }

    public static final class Builder {

        private BtMpxClient btMpxClient;
        private BtAllChannelsChannelGroupUpdater allChannelsGroupUpdater;
        private ChannelGroupResolver channelGroupResolver;
        private Publisher publisher;
        private ChannelGroupWriter channelGroupWriter;
        private ChannelResolver channelResolver;
        private ChannelWriter channelWriter;
        private Lock channelWriterLock;
        private BtChannelDataUpdater channelDataUpdater;
        private boolean ingestAdvertiseFromField;
        private String aliasUriPrefix;
        private String aliasNamespacePrefix;

        private Builder() {}

        public Builder withBtMpxClient(BtMpxClient btMpxClient) {
            this.btMpxClient = btMpxClient;
            return this;
        }

        public Builder withAllChannelsGroupUpdater(
                BtAllChannelsChannelGroupUpdater allChannelsGroupUpdater) {
            this.allChannelsGroupUpdater = allChannelsGroupUpdater;
            return this;
        }

        public Builder withChannelGroupResolver(ChannelGroupResolver channelGroupResolver) {
            this.channelGroupResolver = channelGroupResolver;
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withChannelGroupWriter(ChannelGroupWriter channelGroupWriter) {
            this.channelGroupWriter = channelGroupWriter;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withChannelWriter(ChannelWriter channelWriter) {
            this.channelWriter = channelWriter;
            return this;
        }

        public Builder withChannelWriterLock(Lock channelWriterLock) {
            this.channelWriterLock = channelWriterLock;
            return this;
        }

        public Builder withChannelDataUpdater(BtChannelDataUpdater channelDataUpdater) {
            this.channelDataUpdater = channelDataUpdater;
            return this;
        }

        public Builder withIngestAdvertiseFromField(boolean ingestAdvertiseFromField) {
            this.ingestAdvertiseFromField = ingestAdvertiseFromField;
            return this;
        }

        public Builder withAliasUriPrefix(String aliasUriPrefix) {
            this.aliasUriPrefix = aliasUriPrefix;
            return this;
        }

        public Builder withAliasNamespacePrefix(String aliasNamespacePrefix) {
            this.aliasNamespacePrefix = aliasNamespacePrefix;
            return this;
        }

        public BtMpxChannelDataIngester build() {
            return new BtMpxChannelDataIngester(this);
        }
    }
}
