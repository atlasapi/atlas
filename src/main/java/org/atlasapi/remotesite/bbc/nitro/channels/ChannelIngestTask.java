package org.atlasapi.remotesite.bbc.nitro.channels;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.remotesite.bbc.nitro.NitroChannelAdapter;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIngestTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ChannelIngestTask.class);

    private final NitroChannelAdapter channelAdapter;
    private final NitroChannelHydrator hidrator;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;

    private ChannelIngestTask(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter, ChannelResolver channelResolver, NitroChannelHydrator hidrator) {
        this.channelAdapter = checkNotNull(channelAdapter);
        this.channelWriter = checkNotNull(channelWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.hidrator = checkNotNull(hidrator);
    }

    public static ChannelIngestTask create(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter, ChannelResolver channelResolver, NitroChannelHydrator hidrator) {
        return new ChannelIngestTask(channelAdapter, channelWriter, channelResolver, hidrator);
    }

    @Override
    protected void runTask() {
        try {
            reportStatus("Fetching channels");
            ImmutableSet<Channel> services = channelAdapter.fetchServices();

            reportStatus("Fetching masterbrands");
            ImmutableSet<Channel> masterbrands = channelAdapter.fetchMasterbrands();

            reportStatus("Writing channels");
            Iterable<Channel> filteredServices = hidrator.filterAndHydrateServices(services);
            writeAndMergeChannels(filteredServices);

            Iterable<Channel> filteredMasterBrands = hidrator.filterAndHydrateMasterbrands(masterbrands);
            writeAndMergeChannels(filteredMasterBrands);

        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        }

    }

    private void writeAndMergeChannels(Iterable<Channel> channels) {
        int failed = 0;
        for (Channel channel : channels) {
            Maybe<Channel> existing = channelResolver.fromUri(channel.getCanonicalUri());
            try {
                if (existing.hasValue()) {
                    Channel existingChannel = existing.requireValue();

                    existingChannel.setTitles(channel.getAllTitles());
                    existingChannel.setAdult(channel.getAdult());
                    existingChannel.setImages(channel.getAllImages());
                    existingChannel.setStartDate(channel.getStartDate());
                    existingChannel.setEndDate(channel.getEndDate());
                    existingChannel.setRelatedLinks(channel.getRelatedLinks());
                    existingChannel.addAliasUrls(channel.getAliasUrls());
                    existingChannel.setParent(channel.getParent());
                    existingChannel.setMediaType(channel.getMediaType());
                    existingChannel.setHighDefinition(channel.getHighDefinition());
                    existingChannel.setRegional(channel.getRegional());
                    existingChannel.setTimeshift(channel.getTimeshift());
                    existingChannel.setGenres(channel.getGenres());
                    existingChannel.setAvailableFrom(Sets.union(existingChannel.getAvailableFrom(), channel.getAvailableFrom()));
                    existingChannel.setShortDescription(channel.getShortDescription());
                    existingChannel.setMediumDescription(channel.getMediumDescription());
                    existingChannel.setLongDescription(channel.getLongDescription());
                    existingChannel.setChannelType(channel.getChannelType());
                    existingChannel.setRegion(channel.getRegion());
                    existingChannel.setInteractive(channel.getInteractive());
                    existingChannel.addAliases(channel.getAliases());

                    channelWriter.createOrUpdate(existingChannel);
                } else {
                    channelWriter.createOrUpdate(channel);
                }
            } catch (Exception e) {
                log.error("Failed to write channel {} - {}", channel.getCanonicalUri(), e);
                ++failed;
            }
        }
        reportStatus(String.format("%d channels failed out of %d", failed,
                Iterables.size(channels)
        ));
    }
}
