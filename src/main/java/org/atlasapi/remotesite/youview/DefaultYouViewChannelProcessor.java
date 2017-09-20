package org.atlasapi.remotesite.youview;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.metabroadcast.common.scheduling.UpdateProgress;
import nu.xom.Elements;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        
        List<ItemRefAndBroadcast> broadcasts = Lists.newArrayList();
        Builder<String, String> acceptableBroadcastIds = ImmutableMap.builder();
        
        UpdateProgress progress = UpdateProgress.START;
        for (int i = 0; i < elements.size(); i++) {
            try {
                ItemRefAndBroadcast itemAndBroadcast = processor.process(
                        channel,
                        targetPublisher,
                        elements.get(i)
                );
                broadcasts.add(itemAndBroadcast);
                acceptableBroadcastIds.put(
                        itemAndBroadcast.getBroadcast().getSourceId(),
                        itemAndBroadcast.getItemUri()
                );
                progress = progress.reduce(UpdateProgress.SUCCESS);
            } catch (Exception e) {
                log.error(String.format("Failed to process element: %s", elements.get(i).toXML()), e);
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
            if (log.isInfoEnabled()) {
                log.info("No broadcasts for channel {} ({}) on {}",
                        channel.getTitle(),
                        getYouViewId(channel),
                        schedulePeriod.getStart());
            }
        } else {
            try {
                scheduleWriter.replaceScheduleBlock(targetPublisher, channel, broadcasts);
            } catch (IllegalArgumentException e) {
                log.error(String.format("Failed to update schedule for channel %s (%s) on %s: %s", 
                        channel.getTitle(), getYouViewId(channel), 
                        schedulePeriod.getStart().toString(), e.getMessage()), e);
            }
        }
        
        return progress;
    }

    private String getYouViewId(Channel channel) {
        for (String alias : channel.getAliasUrls()) {
            if (alias.contains("youview.com")) {
                return alias;
            }
        }
        throw new IllegalArgumentException(
                "Channel " + channel.getTitle()
                + " does not have a YouView alias (" + channel.toString() + ")"
        );
    }
}
