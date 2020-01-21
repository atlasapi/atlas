package org.atlasapi.remotesite.youview;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.metabroadcast.common.scheduling.UpdateProgress;
import nu.xom.Element;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    public Map.Entry<UpdateProgress, Set<String>> process(Channel channel, Publisher targetPublisher,
                                      List<Element> elements) {
        
        Builder<String, String> acceptableBroadcastIds = ImmutableMap.builder();

        Set<String> contentUris = new HashSet<>();

        List<Stopwatch> processElementStopwatches = new ArrayList<>();
        List<Stopwatch> trimBroadcastStopwatches = new ArrayList<>();
        List<Stopwatch> replaceScheduleBlockStopwatches = new ArrayList<>();

        UpdateProgress progress = UpdateProgress.START;
        for (Element element : elements) {
            try {

                Stopwatch processElementStopwatch = Stopwatch.createStarted();
                ItemRefAndBroadcast itemAndBroadcast = processor.process(
                        channel,
                        targetPublisher,
                        element
                );
                processElementStopwatch.stop();
                processElementStopwatches.add(processElementStopwatch);

                contentUris.add(itemAndBroadcast.getItemUri());

                Broadcast broadcast = itemAndBroadcast.getBroadcast();

                Interval broadcastInterval = new Interval(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime());

                acceptableBroadcastIds.put(
                        broadcast.getSourceId(),
                        itemAndBroadcast.getItemUri()
                );


                Stopwatch trimBroadcastStopwatch = Stopwatch.createStarted();
                if (trimmer != null) {
                    trimmer.trimBroadcasts(
                            targetPublisher,
                            broadcastInterval,
                            channel,
                            acceptableBroadcastIds.build()
                    );
                }
                trimBroadcastStopwatch.stop();
                trimBroadcastStopwatches.add(trimBroadcastStopwatch);

                try {
                    Stopwatch replaceScheduleBlockStopwatch = Stopwatch.createStarted();
                    scheduleWriter.replaceScheduleBlock(targetPublisher, channel, ImmutableList.of(itemAndBroadcast));
                    replaceScheduleBlockStopwatch.stop();
                    replaceScheduleBlockStopwatches.add(replaceScheduleBlockStopwatch);
                } catch (IllegalArgumentException e) {
                    log.error(String.format("Failed to update schedule for channel %s (%s) on %s: %s",
                            channel.getTitle(), getYouViewId(channel),
                            broadcastInterval.getStart().toString(), e.getMessage()), e);
                }

                progress = progress.reduce(UpdateProgress.SUCCESS);
            } catch (Exception e) {
                log.error(String.format("Failed to process element: %s", element.toXML()), e);
                progress = progress.reduce(UpdateProgress.FAILURE);
            }
        }

        if (elements.size() > 0) {

            long processElementAverageTime = processElementStopwatches.stream()
                    .map(stopwatch -> stopwatch.elapsed(TimeUnit.MILLISECONDS))
                    .mapToLong(Long::longValue)
                    .sum()
                    / processElementStopwatches.size();

            log.info("Average processElement time {} over {} calls", processElementAverageTime, processElementStopwatches.size());

            long trimBroadcastAverageTime = trimBroadcastStopwatches.stream()
                    .map(stopwatch -> stopwatch.elapsed(TimeUnit.MILLISECONDS))
                    .mapToLong(Long::longValue)
                    .sum()
                    / trimBroadcastStopwatches.size();

            log.info("Average trimBroadcast time {} over {} calls", trimBroadcastAverageTime, trimBroadcastStopwatches.size());

            long replaceScheduleBlockAverageTime = replaceScheduleBlockStopwatches.stream()
                    .map(stopwatch -> stopwatch.elapsed(TimeUnit.MILLISECONDS))
                    .mapToLong(Long::longValue)
                    .sum()
                    / replaceScheduleBlockStopwatches.size();

            log.info("Average replaceScheduleBlock time {} over {} calls", replaceScheduleBlockAverageTime, replaceScheduleBlockStopwatches.size());
        }

        
        return new HashMap.SimpleImmutableEntry<>(progress, contentUris);
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
