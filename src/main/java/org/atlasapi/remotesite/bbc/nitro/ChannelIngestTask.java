package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIngestTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ChannelIngestTask.class);

    private final NitroChannelAdapter channelAdapter;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;

    private ChannelIngestTask(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter, ChannelResolver channelResolver) {
        this.channelAdapter = checkNotNull(channelAdapter);
        this.channelWriter = checkNotNull(channelWriter);
        this.channelResolver = checkNotNull(channelResolver);
    }

    public static ChannelIngestTask create(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter, ChannelResolver channelResolver) {
        return new ChannelIngestTask(channelAdapter, channelWriter, channelResolver);
    }

    @Override
    protected void runTask() {
        try {
            reportStatus("Fetching channels");
            ImmutableSet<Channel> services = channelAdapter.fetchServices();

            reportStatus("Fetching masterbrands");
            ImmutableSet<Channel> masterbrands = channelAdapter.fetchMasterbrands();

            reportStatus("Writing channels");
            int failedServices = 0;
            writeAndMergeChannels(services, failedServices);
            int servicesCount = services.size();
            reportStatus(String.format("%d services failed out of %d", failedServices,
                    servicesCount
            ));

            int failedMasterbrands = 0;
            writeAndMergeChannels(masterbrands, failedMasterbrands);
            int masterbrandsCount = masterbrands.size();
            reportStatus(String.format("%d masterbrands failed out of %d", failedMasterbrands,
                    masterbrandsCount
            ));

            reportStatus(String.format("%d failed services and masterbrands out of %d", failedMasterbrands+failedServices, servicesCount
                    + masterbrandsCount));
        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        }

    }

    private void writeAndMergeChannels(Iterable<Channel> channels, int failedCount) {
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
                    channelWriter.createOrUpdate(existingChannel);
                } else {
                    channelWriter.createOrUpdate(channel);
                }
            } catch (Exception e) {
                log.error("Failed to write channel {}", channel.getCanonicalUri());
                failedCount ++;
            }
        }
    }
}
