package org.atlasapi.remotesite.youview;

import java.util.List;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.atlasapi.telescope.TelescopeFactory;
import org.atlasapi.telescope.TelescopeProxy;

import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import nu.xom.Elements;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultYouViewChannelProcessor implements YouViewChannelProcessor {

    private static final Logger log = LoggerFactory.getLogger(DefaultYouViewChannelProcessor.class);

    private final ScheduleWriter scheduleWriter;
    private final YouViewElementProcessor processor;
    private final BroadcastTrimmer trimmer;

    public DefaultYouViewChannelProcessor(ScheduleWriter scheduleWriter,
            YouViewElementProcessor processor, BroadcastTrimmer trimmer) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.processor = checkNotNull(processor);
        this.trimmer = checkNotNull(trimmer);
    }

    @Override
    public UpdateProgress process(Channel channel, Publisher targetPublisher,
            Elements elements, Interval schedulePeriod) {

        //get a telescope client and start reporting
        TelescopeProxy telescope = TelescopeFactory.make(TelescopeFactory.IngesterName.YOU_VIEW);
        telescope.startReporting();

        List<ItemRefAndBroadcast> broadcasts = Lists.newArrayList();
        Builder<String, String> acceptableBroadcastIds = ImmutableMap.builder();

        UpdateProgress progress = UpdateProgress.START;
        for (int i = 0; i < elements.size(); i++) {
            try {
                Item item = processor.process(targetPublisher, elements.get(i));
                ItemRefAndBroadcast itemAndBroadcast = new ItemRefAndBroadcast(
                        item,
                        DefaultYouViewElementProcessor.getBroadcastFromItem(item)
                );

                //Report to telescope logic
                if (item.getId() != null) {
                    telescope.reportSuccessfulEvent(item.getId(), item.getAliases(), elements.get(i));
                } else {
                    telescope.reportFailedEventWithWarning(
                            "Atlas did not return an id after attempting to create or update this Item (targetPublisher="+targetPublisher.key()+")",
                            elements.get(i)
                    );
                }

                if (itemAndBroadcast != null) {
                    broadcasts.add(itemAndBroadcast);
                    acceptableBroadcastIds.put(
                            itemAndBroadcast.getBroadcast().getSourceId(),
                            itemAndBroadcast.getItemUri()
                    );
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } else {
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
            } catch (Exception e) {
                log.error(
                        String.format("Failed to process element: %s", elements.get(i).toXML()),
                        e
                );
                telescope.reportFailedEventWithError("There was an exception while processing this element (targetPublisher="+targetPublisher.key()+")", elements.get(i));
                progress = progress.reduce(UpdateProgress.FAILURE);
            }
        }

        if (trimmer != null) {
            trimmer.trimBroadcasts(
                    targetPublisher,
                    schedulePeriod,
                    channel,
                    acceptableBroadcastIds.build()
            );
        }

        if (broadcasts.isEmpty()) {
            log.info(String.format("No broadcasts for channel %s (%s) on %s", channel.getTitle(),
                    getYouViewId(channel), schedulePeriod.getStart().toString()
            ));
        } else {
            try {
                scheduleWriter.replaceScheduleBlock(targetPublisher, channel, broadcasts);
            } catch (IllegalArgumentException e) {
                log.error(String.format("Failed to update schedule for channel %s (%s) on %s: %s",
                        channel.getTitle(), getYouViewId(channel),
                        schedulePeriod.getStart().toString(), e.getMessage()
                ), e);
            }
        }

        telescope.endReporting();

        return progress;
    }

    private String getYouViewId(Channel channel) {
        for (String alias : channel.getAliasUrls()) {
            if (alias.contains("youview.com")) {
                return alias;
            }
        }
        throw new RuntimeException("Channel "
                + channel.getTitle()
                + " does not have a YouView alias ("
                + channel.toString()
                + ")");
    }
}
