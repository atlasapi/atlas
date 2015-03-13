package org.atlasapi.remotesite.itv.interlinking;

import static org.atlasapi.remotesite.itv.interlinking.XomElement.getAttrValue;
import static org.atlasapi.remotesite.itv.interlinking.XomElement.getElemValue;
import static org.atlasapi.remotesite.itv.interlinking.XomElement.requireElemValue;

import java.util.Set;

import nu.xom.Element;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.text.MoreStrings;

public class ItvInterlinkingContentExtractor {
    
    public static final String ATOM_NS = "http://www.w3.org/2005/Atom";
    public static final String INTERLINKING_NS = "http://www.bbc.co.uk/developer/interlinking";
    public static final String MEDIA_NS = "http://search.yahoo.com/mrss/";
    
    private static final Splitter keywordSplitter = Splitter.on(",").trimResults();
    private final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTimeParser();
    private final PeriodFormatter periodFormatter = ISOPeriodFormat.standard();
    private final ItvInterlinkingChannelMap channelMap;
    private final ContentResolver contentResolver;
    
    public ItvInterlinkingContentExtractor(ContentResolver contentResolver, ChannelResolver channelResolver) {
        this.contentResolver = contentResolver;
        channelMap = new ItvInterlinkingChannelMap(channelResolver);
    }
    
    public InterlinkingEntry<Brand> getBrand(Element brandElem) {
        
        String id = requireElemValue(brandElem, "id", ATOM_NS);
        Brand brand = new Brand(id, getCurie(id), Publisher.ITV_INTERLINKING);
        brand.setTitle(requireElemValue(brandElem, "title", ATOM_NS));
        
        Element contentElem = brandElem.getFirstChildElement("content", ATOM_NS).getFirstChildElement("content", MEDIA_NS);
        Maybe<String> description = getElemValue(contentElem, "description", MEDIA_NS);
        if (description.hasValue()) {
            brand.setDescription(description.requireValue());
        }
        Maybe<String> keywords = getElemValue(contentElem, "keywords", MEDIA_NS);
        if (keywords.hasValue()) {
            brand.setTags(ImmutableSet.copyOf(Iterables.transform(keywordSplitter.split(keywords.requireValue()), MoreStrings.TO_LOWER)));
        }
        
        Maybe<String> image = getAttrValue(contentElem, "thumbnail", MEDIA_NS, "url");
        if (image.hasValue()) {
            brand.setImage(image.requireValue());
            brand.setThumbnail(getThumbnail(image.requireValue()));
        }
        
        return new InterlinkingEntry<Brand>(brand, id);
    }
    
    public InterlinkingEntry<Series> getSeries(Element seriesElem) {
        
        String id = requireElemValue(seriesElem, "id", ATOM_NS);
        Series series = new Series(id, getCurie(id), Publisher.ITV_INTERLINKING);
        
        Element contentElem = seriesElem.getFirstChildElement("content", ATOM_NS).getFirstChildElement("content", MEDIA_NS);
        Maybe<String> description = getElemValue(contentElem, "description", MEDIA_NS);
        if (description.hasValue()) {
            series.setDescription(description.requireValue());
        }
        series.setPublisher(Publisher.ITV_INTERLINKING);
        Maybe<String> index = getElemValue(contentElem, "index", INTERLINKING_NS);
        Maybe<String> parentId = getElemValue(contentElem, "parent_id", INTERLINKING_NS);
        
        if (parentId.hasValue()) {
            if (index.hasValue()) {
                return new InterlinkingEntry<Series>(series, id, parentId.requireValue(), Integer.parseInt(index.requireValue()));
            } else {
                return new InterlinkingEntry<Series>(series, id, parentId.requireValue());
            }
            
        } else {
            return new InterlinkingEntry<Series>(series, id);
        }
    }
    
    public void getSubSeries(Element subSeriesElem) {
        throw new RuntimeException("subseries!" + subSeriesElem.toXML());
    }   

    public InterlinkingEntry<? extends Item> getEpisode(Element episodeElem) {
        
        String id = requireElemValue(episodeElem, "id", ATOM_NS);
        
        Maybe<Identified> existingContent = contentResolver.findByCanonicalUris(ImmutableList.of(id)).get(id);
        
        String title = requireElemValue(episodeElem, "title", ATOM_NS);
        
        String link = episodeElem.getFirstChildElement("link", ATOM_NS).getAttributeValue("href");
        
        Element contentElem = episodeElem.getFirstChildElement("content", ATOM_NS).getFirstChildElement("content", MEDIA_NS);
        
        Maybe<String> description = getElemValue(contentElem, "description", MEDIA_NS);
        
        Set<String> tags = Sets.newHashSet();
        Maybe<String> keywords = getElemValue(contentElem, "keywords", MEDIA_NS);
        if (keywords.hasValue()) {
            Iterables.addAll(tags, Iterables.transform(keywordSplitter.split(keywords.requireValue()), MoreStrings.TO_LOWER));
        }
        
        Maybe<String> image = getAttrValue(contentElem, "thumbnail", MEDIA_NS, "url");
        
        Maybe<String> parentId = getElemValue(contentElem, "parent_id", INTERLINKING_NS);
        
        Maybe<String> index = getElemValue(contentElem, "index", INTERLINKING_NS);
            
        Item item;
        if (existingContent.hasValue()) {
            if (existingContent.requireValue() instanceof Item) {
                item = (Item) existingContent.requireValue();
            }
            else {
                throw new RuntimeException("Existing item was not the correct type");
            }
        }
        if (parentId.hasValue()) {
            Episode episode = new Episode(id, getCurie(id), Publisher.ITV_INTERLINKING);
            
            if (index.hasValue()) {
                episode.setEpisodeNumber(Integer.parseInt(index.requireValue()));
            }
            
            item = episode;
        } else {
            item = new Item(id, getCurie(id), Publisher.ITV_INTERLINKING);
        }
        
        item.setTitle(title);
        if (description.hasValue()) {
            item.setDescription(description.requireValue());
        }
        item.setTags(tags);
        if (image.hasValue()) {
            item.setImage(image.requireValue());
            item.setThumbnail(getThumbnail(image.requireValue()));
        }
        
        if (parentId.hasValue()) {
            return new InterlinkingEntry<Item>(item, id, parentId.requireValue()).withLink(link);
        } else {
            return new InterlinkingEntry<Item>(item, id).withLink(link);
        }
    }
    
    public InterlinkingEntry<Broadcast> getBroadcast(Element broadcastElem) {
        
        String id = requireElemValue(broadcastElem, "id", ATOM_NS);
        
        Element contentElem = broadcastElem.getFirstChildElement("content", ATOM_NS).getFirstChildElement("content", MEDIA_NS);
        String parentId = requireElemValue(contentElem, "parent_id", INTERLINKING_NS);
        
        DateTime startTime = dateFormatter.parseDateTime(requireElemValue(contentElem, "broadcast_start", INTERLINKING_NS));
        Duration duration = periodFormatter.parsePeriod(requireElemValue(contentElem, "duration", INTERLINKING_NS)).toStandardDuration();
        Channel channel = channelMap.get(requireElemValue(contentElem, "service", INTERLINKING_NS));
        
        Broadcast broadcast = new Broadcast(channel.getUri(), startTime, duration);
        broadcast.withId(id);
        
        return new InterlinkingEntry<Broadcast>(broadcast, id, parentId);
    }
    
    private String getThumbnail(String imageUrl) {
        return imageUrl.substring(0, imageUrl.lastIndexOf("?w=")) + "?w=172";
    }
    
    public InterlinkingEntry<Version> getOnDemand(Element ondemandElem) {
        Version version = new Version();
        
        String id = requireElemValue(ondemandElem, "id", ATOM_NS);
        Element contentElem = ondemandElem.getFirstChildElement("content", ATOM_NS).getFirstChildElement("content", MEDIA_NS);
        String parentId = requireElemValue(contentElem, "parent_id", INTERLINKING_NS);
        
        version.setDuration(periodFormatter.parsePeriod(requireElemValue(contentElem, "duration", INTERLINKING_NS)).toStandardDuration());
        version.setProvider(Publisher.ITV_INTERLINKING);
        
        Encoding encoding = new Encoding();
        Location location = new Location();
        location.setTransportType(TransportType.LINK);
        
        Policy policy = new Policy();
        DateTime availabilityStart = dateFormatter.parseDateTime(requireElemValue(contentElem, "availability_start", INTERLINKING_NS));
        policy.setAvailabilityStart(availabilityStart);
        DateTime availabilityEnd = dateFormatter.parseDateTime(requireElemValue(contentElem, "availability_end", INTERLINKING_NS));
        policy.setAvailabilityEnd(availabilityEnd);
        policy.setRevenueContract(RevenueContract.FREE_TO_VIEW);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        
        location.setPolicy(policy);
        encoding.addAvailableAt(location);
        version.addManifestedAs(encoding);
        
        return new InterlinkingEntry<Version>(version, id, parentId);
    }

    private String getCurie(String id) {
        return "itv:" + id.substring("http://itv.com/".length()).replace("/", "-");
    }
}
