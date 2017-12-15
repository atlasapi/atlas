package org.atlasapi.remotesite.bbc.nitro.channels;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.NewAlert;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.remotesite.bbc.nitro.ModelWithPayload;
import org.atlasapi.remotesite.bbc.nitro.NitroChannelAdapter;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.status.util.Utils.encode;
import static org.atlasapi.reporting.status.Utils.getPartialStatusForContent;

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
        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.BBC_NITRO_INGEST_CHANNELS,
                Event.Type.INGEST
        );
        OwlReporter owlReporter = new OwlReporter(telescope);
        owlReporter.getTelescopeReporter().startReporting();

        try {
            progress = UpdateProgress.START;

            ImmutableSet<ModelWithPayload<Channel>> masterbrands = channelAdapter.fetchMasterbrands();
            Iterable<ModelWithPayload<Channel>> filteredMasterBrands = hydrator.hydrateMasterbrands(masterbrands);

            ImmutableMap.Builder<String, Channel> uriToId = ImmutableMap.builder();
            for (Channel channel : writeAndMergeChannels(filteredMasterBrands, owlReporter)) {
                uriToId.put(channel.getUri(), channel);
            }

            log.info("Wrote masterbrands, moving onto channels");

            ImmutableList<ModelWithPayload<Channel>> services = channelAdapter.fetchServices(uriToId.build());
            Iterable<ModelWithPayload<Channel>> filteredServices = hydrator.hydrateServices(services);

            ImmutableList.Builder<ModelWithPayload<Channel>> withUris = ImmutableList.builder();
            for (ModelWithPayload<Channel> channel : filteredServices) {
                if (channel.getModel().getCanonicalUri() != null) {
                    withUris.add(channel);
                } else {
                    log.warn(
                            "Got channel without URI; this generally means it has no DVB locator "
                                    + "in Nitro and no hard-coded override. Ask the BBC for the "
                                    + "corresponding DVB locator. {}",
                            channel.getModel().getAliases()
                    );
                }
            }

            writeAndMergeChannels(withUris.build(), owlReporter);

            reportStatus(progress.toString());
        } catch (GlycerinException e) {
            owlReporter.getTelescopeReporter().reportFailedEvent(
                    "A Glycerin exception prevented this channel ingest task from running properly."
                    + " (" + e.toString() + ")");
            throw Throwables.propagate(e);
        } finally {
            owlReporter.getTelescopeReporter().endReporting();
        }
    }

    private Iterable<Channel> writeAndMergeChannels(
            Iterable<ModelWithPayload<Channel>> channels,
            OwlReporter owlReporter) {

        ImmutableList.Builder<Channel> written = ImmutableList.builder();
        for (ModelWithPayload<Channel> channelWithPayload : channels) {
            Channel channel = channelWithPayload.getModel();
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
                    existingChannel.setIsTimeshifted(channel.isTimeshifted());
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
                    owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                            existingChannel.getId(),
                            existingChannel.getAliases(),
                            EntityType.CHANNEL,
                            channelWithPayload.getPayload());

                    if (Strings.isNullOrEmpty(existingChannel.getTitle())){
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CHANNEL,
                                existingChannel.getId(),
                                getPartialStatusForContent(
                                        existingChannel.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        String.format("Channel %s is missing a title.",
                                                encode(existingChannel.getId())
                                        ),
                                        EntityRef.Type.CHANNEL,
                                        existingChannel.getSource().key(),
                                        false
                                )
                        );
                    } else {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CHANNEL,
                                existingChannel.getId(),
                                getPartialStatusForContent(
                                        existingChannel.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        null,
                                        EntityRef.Type.CHANNEL,
                                        existingChannel.getSource().key(),
                                        true
                                )
                        );
                    }

                    log.debug("Writing merged channel {}", existingChannel);
                } else {
                    written.add(channelWriter.createOrUpdate(channel));
                    owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                            channel.getId(),
                            channel.getAliases(),
                            EntityType.CHANNEL,
                            channelWithPayload.getPayload());

                    if (Strings.isNullOrEmpty(channel.getTitle())){
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CHANNEL,
                                channel.getId(),
                                getPartialStatusForContent(
                                        channel.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        String.format("Channel %s is missing a title.",
                                                encode(channel.getId())
                                        ),
                                        EntityRef.Type.CHANNEL,
                                        channel.getSource().key(),
                                        false
                                )
                        );
                    } else {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CHANNEL,
                                channel.getId(),
                                getPartialStatusForContent(
                                        channel.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        null,
                                        EntityRef.Type.CHANNEL,
                                        channel.getSource().key(),
                                        true
                                )
                        );
                    }
                }
                progress = progress.reduce(UpdateProgress.SUCCESS);
            } catch (Exception e) {
                log.error("Failed to write channel {} - {}", channel.getCanonicalUri(), e);
                owlReporter.getTelescopeReporter().reportFailedEvent(
                        "Failed to write channel (" + e.toString() + ")",
                        channelWithPayload.getPayload());
                progress = progress.reduce(UpdateProgress.FAILURE);
            }
        }
        return written.build();
    }
}
