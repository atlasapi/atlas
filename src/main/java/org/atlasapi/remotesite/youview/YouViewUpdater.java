package org.atlasapi.remotesite.youview;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.youview.YouViewChannelResolver.ServiceId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.youview.YouViewContentExtractor.DURATION_KEY;
import static org.atlasapi.remotesite.youview.YouViewContentExtractor.MEDIA_CONTENT_KEY;
import static org.atlasapi.remotesite.youview.YouViewContentExtractor.MEDIA_PREFIX;
import static org.atlasapi.remotesite.youview.YouViewContentExtractor.SCHEDULE_SLOT_KEY;
import static org.atlasapi.remotesite.youview.YouViewContentExtractor.YV_PREFIX;

public class YouViewUpdater extends ScheduledTask {

    private static final String ATOM_PREFIX = "atom";
    private static final String ENTRY_KEY = "entry";
    private static final String DATE_TIME_FORMAT = "YYYY-MM-dd'T'HH:mm:ss'Z'";
    private final YouViewScheduleFetcher fetcher;
    private final int plusDays;
    private final int minusDays;
    private final Logger log = LoggerFactory.getLogger(YouViewUpdater.class);
    private final YouViewChannelResolver channelResolver;
    private final YouViewChannelProcessor processor;
    private final YouViewIngestConfiguration ingestConfiguration;
    private final boolean polling;
    private final int hours;

    private static final Joiner joiner = Joiner.on("<->");

    public YouViewUpdater(YouViewChannelResolver channelResolver,
                          YouViewScheduleFetcher fetcher, YouViewChannelProcessor processor,
                          YouViewIngestConfiguration ingestConfiguration,
                          int minusDays, int plusDays) {
        this(channelResolver, fetcher, processor, ingestConfiguration, minusDays, plusDays, false, 0);

    }
    
    public YouViewUpdater(YouViewChannelResolver channelResolver, 
            YouViewScheduleFetcher fetcher, YouViewChannelProcessor processor,
            YouViewIngestConfiguration ingestConfiguration,
            int minusDays, int plusDays, boolean polling, int hours) {
        this.channelResolver = checkNotNull(channelResolver);
        this.fetcher = checkNotNull(fetcher);
        this.processor = checkNotNull(processor);
        this.ingestConfiguration = checkNotNull(ingestConfiguration);
        this.minusDays = minusDays;
        this.plusDays = plusDays;
        this.polling = polling;
        this.hours = hours;
    }

    @Override
    protected void runTask() {
        if (!polling) {
            runRegularTask();
        } else {
            runPollingTask();
        }
    }

    // TODO report status effectively
    private void runPollingTask() {
        Multimap<ServiceId, Channel> youViewChannels = channelResolver.getAllServiceIdsToChannels();

        UpdateProgress progress = UpdateProgress.START;

        Map<String, LocalDateTime> seenHashes = new HashMap<>(1000);

        try {

            while (this.shouldContinue()) {
                log.info("Polling for YV schedule changes");

                LocalDateTime now = LocalDateTime.now(DateTimeZone.UTC);
                LocalDateTime to = now.plusHours(hours);
                Interval interval = new Interval(now.toDateTime(DateTimeZone.UTC), to.toDateTime(DateTimeZone.UTC));

                int count = 0;
                int unchangedElements = 0;

                for (Entry<ServiceId, Channel> entry : youViewChannels.entries()) {
                    ServiceId serviceId = entry.getKey();
                    Channel channel = entry.getValue();
                    DateTime startDate = interval.getStart();
                    DateTime endDate = interval.getEnd();
                    Document xml = fetcher.getSchedule(startDate, endDate, serviceId.getId());
                    Element root = xml.getRootElement();
                    Elements entries = root.getChildElements(ENTRY_KEY, root.getNamespaceURI(ATOM_PREFIX));

                    if (entries.size() == 0 && log.isWarnEnabled()) {
                        log.warn("Schedule for {}?starttime={}&endtime={}&service={} is empty for {} ({})",
                                fetcher.getBaseUrl(),
                                startDate.toString(DATE_TIME_FORMAT),
                                endDate.toString(DATE_TIME_FORMAT),
                                serviceId,
                                channel,
                                channel.getTitle());
                    }

                    Publisher publisher = publisherFor(serviceId);
                    if (publisher == null) {
                        log.warn("Could not find publisher the channel {} ({}) should be written as (from: {})",
                                channel, channel.getTitle(), serviceId);
                        progress = progress.reduce(UpdateProgress.FAILURE);
                        continue;
                    }


                    ImmutableList.Builder<Element> filteredElements = ImmutableList.builder();

                    for (int i = 0 ; i < entries.size(); i++) {
                        Element element = entries.get(i);
                        String hash = element.getAttribute("hash", element.getNamespaceURI("yv")).getValue();
                        String txStart = getTransmissionTime(element);
                        String duration = getBroadcastDuration(element);
                        String keyedHash = joiner.join(channel.getUri(), txStart, duration, hash);
                        String youviewId = element.getFirstChildElement("id", element.getNamespaceURI(ATOM_PREFIX)).getValue();
                        if (!seenHashes.containsKey(keyedHash)) {
                            log.info("Found new or changed element on {} with id: {}", serviceId, youviewId);
                            filteredElements.add(element);
                        } else {
                            unchangedElements++;
                        }
                        seenHashes.put(keyedHash, LocalDateTime.now());
                    }

                    progress = progress.reduce(processor.process(
                            channel,
                            publisher,
                            filteredElements.build()
                    ));
                    reportStatus(progress.toString());

                    //TODO: this is just for testing to avoid spamming YV
//                    if(count++ >= 20) {
//                        break;
//                    }
                }
                log.info("{} unchanged elements across {} channels", unchangedElements, count);

                Thread.sleep(60000); //TODO: lower this - don't spam too much for now whilst testing
                seenHashes.entrySet().removeIf(entry -> entry.getValue().plusHours(4).isBefore(now));
            }
        } catch (Exception e) {
            log.error("Error whilst running the polling youview updater", e);
            Throwables.propagate(e);
        }
    }

    private String getTransmissionTime(Element source) {
        Element transmissionTime = source.getFirstChildElement(SCHEDULE_SLOT_KEY, source.getNamespaceURI(YV_PREFIX));
        return transmissionTime.getValue();
    }

    private String getBroadcastDuration(Element source) {
        Element mediaContent = source.getFirstChildElement(MEDIA_CONTENT_KEY, source.getNamespaceURI(MEDIA_PREFIX));
        Attribute duration = mediaContent.getAttribute(DURATION_KEY);
        return duration.getValue();
    }
    
    // TODO report status effectively
    private void runRegularTask() {
        try {
            LocalDate today = LocalDate.now(DateTimeZone.UTC);
            LocalDate start = today.minusDays(minusDays);
            LocalDate finish = today.plusDays(plusDays);
            
            Multimap<ServiceId, Channel> youViewChannels = channelResolver.getAllServiceIdsToChannels();
            
            UpdateProgress progress = UpdateProgress.START;
            
            while (!start.isAfter(finish)) {
                LocalDate end = start.plusDays(1);
                for (Entry<ServiceId, Channel> entry : youViewChannels.entries()) {
                    Interval interval = new Interval(start.toDateTimeAtStartOfDay(), 
                            end.toDateTimeAtStartOfDay());
                    ServiceId serviceId = entry.getKey();
                    Channel channel = entry.getValue();
                    DateTime startDate = interval.getStart();
                    DateTime endDate = interval.getEnd();
                    Document xml = fetcher.getSchedule(startDate, endDate, serviceId.getId());
                    Element root = xml.getRootElement();
                    Elements entries = root.getChildElements(ENTRY_KEY, root.getNamespaceURI(ATOM_PREFIX));

                    if (entries.size() == 0 && log.isWarnEnabled()) {
                        log.warn("Schedule for {}?starttime={}&endtime={}&service={} is empty for {} ({})",
                                fetcher.getBaseUrl(),
                                startDate.toString(DATE_TIME_FORMAT),
                                endDate.toString(DATE_TIME_FORMAT),
                                serviceId,
                                channel,
                                channel.getTitle());
                    }

                    Publisher publisher = publisherFor(serviceId);
                    if (publisher == null) {
                        log.warn("Could not find publisher the channel {} ({}) should be written as (from: {})",
                                channel, channel.getTitle(), serviceId);
                        progress = progress.reduce(UpdateProgress.FAILURE);
                        continue;
                    }

                    ImmutableList.Builder<Element> elements = ImmutableList.builder();

                    for (int i = 0; i < entries.size(); i++) {
                        elements.add(entries.get(i));
                    }

                    progress = progress.reduce(processor.process(
                            channel,
                            publisher,
                            elements.build()
                    ));
                    reportStatus(progress.toString());
                }
                start = end;
            }
        } catch (Exception e) {
            log.error("Exception when processing YouView schedule", e);
            Throwables.propagate(e);
        }

    }

    @Nullable
    private Publisher publisherFor(ServiceId serviceId) {
        return ingestConfiguration.getAliasPrefixToPublisherMap().get(serviceId.getPrefix());
    }
    
}
