package org.atlasapi.remotesite.youview;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

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
    
    public YouViewUpdater(YouViewChannelResolver channelResolver, 
            YouViewScheduleFetcher fetcher, YouViewChannelProcessor processor,
            YouViewIngestConfiguration ingestConfiguration,
            int minusDays, int plusDays) {
        this.channelResolver = checkNotNull(channelResolver);
        this.fetcher = checkNotNull(fetcher);
        this.processor = checkNotNull(processor);
        this.ingestConfiguration = checkNotNull(ingestConfiguration);
        this.minusDays = minusDays;
        this.plusDays = plusDays;
        
    }
    
    // TODO report status effectively
    @Override
    protected void runTask() {
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
                    progress = progress.reduce(processor.process(
                            channel,
                            publisher,
                            entries,
                            interval
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
