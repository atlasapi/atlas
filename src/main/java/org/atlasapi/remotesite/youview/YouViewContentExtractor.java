package org.atlasapi.remotesite.youview;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.AttributeNotFoundException;
import org.atlasapi.remotesite.netflix.ElementNotFoundException;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.intl.Countries;

import com.google.common.collect.ImmutableMap;
import nu.xom.Attribute;
import nu.xom.Element;
import nu.xom.Elements;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class YouViewContentExtractor {

    private static final Pattern NEW_PREFIX = Pattern.compile("^New: ");
    
    private static final String ATOM_PREFIX = "atom";
    private static final String YV_PREFIX = "yv";
    private static final String MEDIA_PREFIX = "media";
    private static final String ID_KEY = "id";
    private static final String TITLE_KEY = "title";
    private static final String PROGRAMME_ID_KEY = "programmeId";
    private static final String IDENTIFIER_KEY = "identifier";
    private static final String PROGRAMME_CRID_KEY = "programmeCRID";
    private static final String SERIES_CRID_KEY = "seriesCRID";
    private static final String PCRID_PREFIX = "pcrid:";
    private static final String SCRID_PREFIX = "scrid:";
    private static final String SERVICE_ID_KEY = "serviceId";
    private static final String EVENT_LOCATOR_KEY = "eventLocator";
    private static final String MEDIA_CONTENT_KEY = "content";
    private static final String DURATION_KEY = "duration";
    private static final String YOUVIEW_PREFIX = "youview:";
    private static final String SCHEDULE_SLOT_KEY = "scheduleSlot";
    private static final String AVAILABLE_KEY = "available";
    private static final String START_KEY = "start";
    private static final String END_KEY = "end";

    private static final OptionalMap<Publisher, Platform> BROADCASTER_TO_PLATFORM =
            ImmutableOptionalMap.fromMap(ImmutableMap.of(
                    Publisher.BBC, Platform.YOUVIEW_IPLAYER,
                    Publisher.ITV, Platform.YOUVIEW_ITVPLAYER,
                    Publisher.C4_PMLSD, Platform.YOUVIEW_4OD,
                    Publisher.FIVE, Platform.YOUVIEW_DEMAND5,
                    Publisher.AMAZON_UNBOX, Platform.YOUVIEW_AMAZON
            ));

    private static final Logger log = LoggerFactory.getLogger(YouViewContentExtractor.class);
            
            
    private final YouViewChannelResolver channelResolver;
    
    private final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTimeNoMillis();
    private final YouViewIngestConfiguration ingestConfiguration;
    
    public YouViewContentExtractor(YouViewChannelResolver channelResolver, YouViewIngestConfiguration ingestConfiguration) {
        this.ingestConfiguration = checkNotNull(ingestConfiguration);
        this.channelResolver = checkNotNull(channelResolver);
    }
    
    public Item extract(Channel channel, Publisher targetPublisher, Element source) {

        Item item = new Item();

        String id = getId(source);
        item.setCanonicalUri(canonicalUriFor(targetPublisher, source));
        item.addAlias(new Alias(getScheduleEventAliasNamespace(), id));
        item.setTitle(getTitle(source));
        item.setMediaType(channel.getMediaType());
        item.setPublisher(targetPublisher);

        Optional<String> programmeId = getProgrammeId(source);
        if (programmeId.isPresent()) {
            item.addAliasUrl(programmeAliasUriFor(targetPublisher, programmeId.get()));
            item.addAlias(new Alias(ingestConfiguration.getAliasNamespacePrefix() 
                    + ":programme", programmeId.get()));
        }
        
        item.addVersion(getVersion(channel, source));
        return item;
    }
    
    public String getScheduleEventAliasNamespace() {
        return ingestConfiguration.getAliasNamespacePrefix() + ":scheduleevent";
    }
    
    /**
     * If there is a programmeCrid present, use that. We're as yet unsure
     * whether programmeCrids are unique over all time, or a limited window
     * so prefix with the tx date. If there's no programmeCrid we use the
     * scheduleEvent id. We use programmeCrids where available to reduce
     * the number of items in the Atlas where programmes are simultaneously
     * broadcast on many regional variants.
     */
    private String canonicalUriFor(Publisher publisher, Element source) {
        Optional<String> programmeCrid = getProgrammeCrid(source);
        
        if (programmeCrid.isPresent()) {
            DateTime transmissionTime = getTransmissionTime(source);
            
            return String.format("http://%s/programmecrid/%d%02d%02d/%s", 
                    publisher.key(), 
                    transmissionTime.getYear(),
                    transmissionTime.getMonthOfYear(),
                    transmissionTime.getDayOfMonth(),
                    programmeCrid.get().replace("crid://", ""));
        } else {
            return scheduleEventUriFor(publisher, getId(source));
        }
    }
    
    private String scheduleEventUriFor(Publisher publisher, String scheduleEventId) {
        return String.format("http://%s/scheduleevent/%s", publisher.key(), scheduleEventId);
    }
    
    private String programmeAliasUriFor(Publisher publisher, String programmeId) {
        return String.format("http://%s/programme/%s", publisher.key(), programmeId);
    }
    
    private Optional<Location> getLocation(Element source, Channel channel) {

        Element available = source.getFirstChildElement(AVAILABLE_KEY, source.getNamespaceURI(YV_PREFIX));
        if (available == null) {
            return Optional.empty();
        }
        
        Attribute start = available.getAttribute(START_KEY);
        Attribute end = available.getAttribute(END_KEY);
        if (start == null || end == null) {
            return Optional.empty();
        }
        
        Policy policy = new Policy();
        policy.setPlatform(getPlatformFor(channel));
        policy.addAvailableCountry(Countries.GB);
        policy.setAvailabilityStart(dateFormatter.parseDateTime(start.getValue()));
        policy.setAvailabilityEnd(dateFormatter.parseDateTime(end.getValue()));

        Location location = new Location();
        location.setPolicy(policy);
        
        return Optional.of(location);
    }

    private Platform getPlatformFor(Channel channel) {
        return BROADCASTER_TO_PLATFORM.get(channel.getBroadcaster()).or(Platform.YOUVIEW);
    }

    private Broadcast getBroadcast(Element source, Channel channel) {
        String id = getBroadcastId(source);
        
        String eventLocator = getEventLocator(source);
        DateTime transmissionTime = getTransmissionTime(source);
        Duration broadcastDuration = getBroadcastDuration(source);
        
        Broadcast broadcast = new Broadcast(channel.getUri(), transmissionTime, transmissionTime.plus(broadcastDuration));
        broadcast.withId(id);
        broadcast.addAliasUrl(eventLocator);
        broadcast.addAlias(new Alias("dvb:event-locator", eventLocator));
        Optional<String> programmeCrid = getProgrammeCrid(source);
        if (programmeCrid.isPresent()) {
            broadcast.addAliasUrl(PCRID_PREFIX + programmeCrid.get());
            broadcast.addAlias(new Alias("dvb:pcrid", programmeCrid.get()));
        }
        Optional<String> seriesCRID = getSeriesCrid(source);
        if (seriesCRID.isPresent()) {
            broadcast.addAliasUrl(SCRID_PREFIX + seriesCRID.get());
            broadcast.addAlias(new Alias("dvb:scrid", seriesCRID.get()));
        }
        
        return broadcast;
    }

    private String getBroadcastId(Element source) {
        Element broadcastId = source.getFirstChildElement(ID_KEY, source.getNamespaceURI(ATOM_PREFIX));
        if (broadcastId == null) {
            throw new ElementNotFoundException(source, ATOM_PREFIX + ":" + ID_KEY);
        }
        return YOUVIEW_PREFIX + broadcastId.getValue();
    }

    private String getServiceId(Element source) {
        Element serviceId = source.getFirstChildElement(SERVICE_ID_KEY, source.getNamespaceURI(YV_PREFIX));
        if (serviceId == null) {
            throw new ElementNotFoundException(source, YV_PREFIX + ":" + SERVICE_ID_KEY);
        }
        return serviceId.getValue();
    }

    private Duration getBroadcastDuration(Element source) {
        Element mediaContent = source.getFirstChildElement(MEDIA_CONTENT_KEY, source.getNamespaceURI(MEDIA_PREFIX));
        if (mediaContent == null) {
            throw new ElementNotFoundException(source, MEDIA_PREFIX + ":" + MEDIA_CONTENT_KEY);
        }
        Attribute duration = mediaContent.getAttribute(DURATION_KEY);
        if (duration == null) {
            throw new AttributeNotFoundException(mediaContent, DURATION_KEY);
        }
        return Duration.standardSeconds(Integer.parseInt(duration.getValue()));
    }

    private String getEventLocator(Element source) {
        Element eventLocator = getElementOfType(source, IDENTIFIER_KEY, YV_PREFIX, EVENT_LOCATOR_KEY);
        if (eventLocator == null) {
            throw new ElementNotFoundException(source, YV_PREFIX + ":" + IDENTIFIER_KEY + " with type: " + EVENT_LOCATOR_KEY);
        }
        return eventLocator.getValue();
    }

    private DateTime getTransmissionTime(Element source) {
        Element transmissionTime = source.getFirstChildElement(SCHEDULE_SLOT_KEY, source.getNamespaceURI(YV_PREFIX));
        if (transmissionTime == null) {
            throw new ElementNotFoundException(source, YV_PREFIX + ":" + SCHEDULE_SLOT_KEY);
        }
        return dateFormatter.parseDateTime(transmissionTime.getValue());
    }

    private Version getVersion(Channel channel, Element source) {
        Optional<Location> location = getLocation(source, channel);

        Version version = new Version();
        version.setDuration(getBroadcastDuration(source));
        version.setPublishedDuration(version.getDuration());
        version.addBroadcast(getBroadcast(source, channel));
        if (location.isPresent()) {
            Encoding encoding = new Encoding();
            encoding.addAvailableAt(location.get());
            version.addManifestedAs(encoding);
        }
        return version;
    }

    private Optional<String> getProgrammeCrid(Element source) {
        Element programmeCrid = getElementOfType(source, IDENTIFIER_KEY, YV_PREFIX, PROGRAMME_CRID_KEY);
        return Optional.ofNullable(programmeCrid).map(Element::getValue);
    }

    private Optional<String> getSeriesCrid(Element source) {
        Element seriesCrid = getElementOfType(source, IDENTIFIER_KEY, YV_PREFIX, SERIES_CRID_KEY);
        return Optional.ofNullable(seriesCrid).map(Element::getValue);
    }

    @Nullable
    private Element getElementOfType(Element source, String elementName, String prefixName, String elementType) {
        Elements elements = source.getChildElements(elementName, source.getNamespaceURI(prefixName));
        for (int i = 0; i < elements.size(); i++) {
            Attribute typeAttr = elements.get(i).getAttribute("type");
            if (typeAttr != null && typeAttr.getValue().contains(elementType)) {
                return elements.get(i);
            }
        }
        return null;
    }

    private Optional<String> getProgrammeId(Element source) {
        Element programmeId = source.getFirstChildElement(PROGRAMME_ID_KEY, source.getNamespaceURI(YV_PREFIX));
        return Optional.ofNullable(programmeId).map(Element::getValue);
    }

    private String getTitle(Element source) {
        Element atomTitle = source.getFirstChildElement(TITLE_KEY, source.getNamespaceURI(ATOM_PREFIX));
        if (atomTitle == null) {
            throw new ElementNotFoundException(source, ATOM_PREFIX + ":" + TITLE_KEY);
        }
        // Items in the schedule can be prefixed "New: " to highlight in the
        // YV epg. There's no clean title field, so we have to remove the 
        // prefix.
        return NEW_PREFIX.matcher(StringEscapeUtils.unescapeHtml(atomTitle.getValue())).replaceAll("");
    }

    private String getId(Element source) {
        Element atomId = source.getFirstChildElement(ID_KEY, source.getNamespaceURI(ATOM_PREFIX));
        if (atomId == null) {
            throw new ElementNotFoundException(source, ATOM_PREFIX + ":" + ID_KEY);
        }
        return atomId.getValue();
    } 
}
