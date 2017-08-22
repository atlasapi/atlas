package org.atlasapi.remotesite.bbc.nitro.channels;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.remotesite.bbc.nitro.NitroChannelAdapter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIngestTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ChannelIngestTask.class);

    private final NitroChannelAdapter channelAdapter;
    private final NitroChannelHydrator hydrator;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;
    private UpdateProgress progress;

    private ChannelIngestTask(
            NitroChannelAdapter channelAdapter,
            ChannelWriter channelWriter,
            ChannelResolver channelResolver,
            NitroChannelHydrator hydrator
    ) {
        this.channelAdapter = checkNotNull(channelAdapter);
        this.channelWriter = checkNotNull(channelWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.hydrator = checkNotNull(hydrator);
    }

    public static ChannelIngestTask create(
            NitroChannelAdapter channelAdapter,
            ChannelWriter channelWriter,
            ChannelResolver channelResolver,
            NitroChannelHydrator hydrator
    ) {
        return new ChannelIngestTask(channelAdapter, channelWriter, channelResolver, hydrator);
    }

    @Override
    protected void runTask() {
        OwlTelescopeReporter telescope = OwlTelescopeReporter.create(
                OwlTelescopeReporters.BBC_NITRO_INGEST_CHANNELS,
                Event.Type.INGEST
        );
        telescope.startReporting();

        try {
            progress = UpdateProgress.START;

            ImmutableSet<Channel> masterbrands = channelAdapter.fetchMasterbrands();
            Iterable<Channel> filteredMasterBrands = hydrator.hydrateMasterbrands(masterbrands);

            ImmutableMap.Builder<String, Channel> uriToId = ImmutableMap.builder();
            for (Channel channel : writeAndMergeChannels(filteredMasterBrands, telescope)) {
                uriToId.put(channel.getUri(), channel);
            }

            log.info("Wrote masterbrands, moving onto channels");

            ImmutableList<Channel> services = channelAdapter.fetchServices(uriToId.build());
            Iterable<Channel> filteredServices = hydrator.hydrateServices(services);

            ImmutableList.Builder<Channel> withUris = ImmutableList.builder();
            for (Channel channel : filteredServices) {
                if (channel.getCanonicalUri() != null) {
                    withUris.add(channel);
                } else {
                    log.warn(
                            "Got channel without URI; this generally means it has no DVB locator "
                                    + "in Nitro and no hard-coded override. Ask the BBC for the "
                                    + "corresponding DVB locator. {}",
                            channel.getAliases()
                    );
                }
            }

            writeAndMergeChannels(withUris.build(), telescope);

            reportStatus(progress.toString());
        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        } finally {
            telescope.endReporting();
        }
    }

    private Iterable<Channel> writeAndMergeChannels(Iterable<Channel> channels, OwlTelescopeReporter telescope) {
        ImmutableList.Builder<Channel> written = ImmutableList.builder();
        for (Channel channel : channels) {
            Maybe<Channel> existing = channelResolver.fromUri(channel.getCanonicalUri());
            try {
                if (existing.hasValue()) {
                    Channel existingChannel = existing.requireValue();

                    // Despite key being deprecated it needs to be set otherwise parts of
                    // Atlas will NPE
                    existingChannel.setKey(channel.getKey());

                    existingChannel.setTitles(channel.getAllTitles());
                    existingChannel.setAdult(channel.getAdult());
                    existingChannel.setImages(channel.getAllImages());
                    existingChannel.setStartDate(channel.getStartDate());
                    existingChannel.setEndDate(channel.getEndDate());
                    existingChannel.setRelatedLinks(channel.getRelatedLinks());
                    existingChannel.setAliasUrls(channel.getAliasUrls());
                    existingChannel.setParent(channel.getParent());
                    existingChannel.setMediaType(channel.getMediaType());
                    existingChannel.setHighDefinition(channel.getHighDefinition());
                    existingChannel.setRegional(channel.getRegional());
                    existingChannel.setTimeshift(channel.getTimeshift());
                    existingChannel.setGenres(channel.getGenres());
                    existingChannel.setAvailableFrom(
                            Sets.union(
                                    existingChannel.getAvailableFrom(), channel.getAvailableFrom()
                            )
                    );
                    existingChannel.setShortDescription(channel.getShortDescription());
                    existingChannel.setMediumDescription(channel.getMediumDescription());
                    existingChannel.setLongDescription(channel.getLongDescription());
                    existingChannel.setChannelType(channel.getChannelType());
                    existingChannel.setRegion(channel.getRegion());
                    existingChannel.setTargetRegions(channel.getTargetRegions());
                    existingChannel.setInteractive(channel.getInteractive());
                    existingChannel.setAliases(channel.getAliases());

                    written.add(channelWriter.createOrUpdate(existingChannel));
                    telescope.reportSuccessfulEvent(existingChannel.getId(), existingChannel.getAliases(), channel);

                    log.debug("Writing merged channel {}", existingChannel);
                } else {
                    written.add(channelWriter.createOrUpdate(channel));
                    telescope.reportSuccessfulEvent(channel.getId(), channel.getAliases(), channel);
                }
                progress = progress.reduce(UpdateProgress.SUCCESS);
            } catch (Exception e) {
                log.error("Failed to write channel {} - {}", channel.getCanonicalUri(), e);
                telescope.reportFailedEvent("Failed to write channel (" + e.getMessage() + ")", channel);
                progress = progress.reduce(UpdateProgress.FAILURE);
            }
        }
        return written.build();
    }
}
