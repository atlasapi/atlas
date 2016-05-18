package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelWriter;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.common.scheduling.ScheduledTask;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIngestTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ChannelIngestTask.class);

    private final NitroChannelAdapter channelAdapter;
    private final ChannelWriter channelWriter;

    private ChannelIngestTask(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter) {
        this.channelAdapter = checkNotNull(channelAdapter);
        this.channelWriter = checkNotNull(channelWriter);
    }

    public static ChannelIngestTask create(NitroChannelAdapter channelAdapter, ChannelWriter channelWriter) {
        return new ChannelIngestTask(channelAdapter, channelWriter);
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
            writeChannels(services, failedServices);
            int servicesCount = services.size();
            reportStatus(String.format("%d services failed out of %d", failedServices,
                    servicesCount
            ));

            int failedMasterbrands = 0;
            writeChannels(masterbrands, failedMasterbrands);
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

    private void writeChannels(Iterable<Channel> channels, int failedCount) {

        for (Channel channel : channels) {
            try {
                channelWriter.createOrUpdate(channel);
            } catch (Exception e) {
                log.error("Failed to write channel {}", channel.getCanonicalUri());
                failedCount ++;
            }
        }
    }
}
