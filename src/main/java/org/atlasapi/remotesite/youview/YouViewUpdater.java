package org.atlasapi.remotesite.youview;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Map.Entry;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;


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
            
            Map<Integer, Channel> youViewChannels = channelResolver.getAllChannelsByServiceId();
            
            UpdateProgress progress = UpdateProgress.START;
            
            while (!start.isAfter(finish)) {
                LocalDate end = start.plusDays(1);
                for (Entry<Integer, Channel> channel : youViewChannels.entrySet()) {
                    Interval interval = new Interval(start.toDateTimeAtStartOfDay(), 
                            end.toDateTimeAtStartOfDay());
                    Integer serviceId = channel.getKey();
                    DateTime startDate = interval.getStart();
                    DateTime endDate = interval.getEnd();
                    Document xml = fetcher.getSchedule(startDate, endDate,
                            serviceId);
                    Element root = xml.getRootElement();
                    Elements entries = root.getChildElements(ENTRY_KEY, root.getNamespaceURI(ATOM_PREFIX));

                    if(entries.size() == 0) {
                        log.warn("Schedule for {}?starttime={}&endtime={}&service={} is empty for channel {}", fetcher.getBaseUrl(),
                                startDate.toString(DATE_TIME_FORMAT), endDate.toString(DATE_TIME_FORMAT), serviceId, channel.getValue().getTitle());
                    }

                    Publisher publisher = publisherFor(channelResolver.getChannelServiceAlias(serviceId));
                    progress = progress.reduce(processor.process(channel.getValue(),  
                            publisher, entries, interval));
                    reportStatus(progress.toString());
                }
                start = end;
            }
        } catch (Exception e) {
            log.error("Exception when processing YouView schedule", e);
            Throwables.propagate(e);
        }

    }
    
    private Publisher publisherFor(String channelServiceAlias) {
        for (Entry<String, Publisher> entry : 
                ingestConfiguration.getAliasPrefixToPublisherMap().entrySet()) {
            if (channelServiceAlias.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new IllegalStateException("Could not find alias prefix to determine which publisher channel should be written as: " + channelServiceAlias);
    }
    
}
