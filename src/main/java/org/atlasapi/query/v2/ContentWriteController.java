package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import com.google.common.collect.ImmutableList;
import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.remotesite.wikipedia.television.ScrapedFlatHierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;

public class ContentWriteController {
    
    //TODO: replace with proper merge strategies.
    private static final boolean MERGE = true;
    private static final boolean OVERWRITE = false;

    private static final Logger log = LoggerFactory.getLogger(ContentWriteController.class);
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ApplicationConfigurationFetcher appConfigFetcher;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ModelReader reader;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final EventResolver eventResolver;

    private ModelTransformer<Description, Content> transformer;

    public ContentWriteController(ApplicationConfigurationFetcher appConfigFetcher,
            ContentResolver resolver, ContentWriter writer, ModelReader reader, 
            ModelTransformer<Description, Content> transformer, ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver, EventResolver eventResolver) {
        this.appConfigFetcher = checkNotNull(appConfigFetcher);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.reader = checkNotNull(reader);
        this.transformer = checkNotNull(transformer);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.eventResolver = checkNotNull(eventResolver);
    }
    
    @RequestMapping(value="/3.0/content.json", method = RequestMethod.POST)
    public Void postContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, MERGE);
    }

    @RequestMapping(value="/3.0/content.json", method = RequestMethod.PUT)
    public Void putContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, OVERWRITE);
    }

    private Void deserializeAndUpdateContent(HttpServletRequest req, HttpServletResponse resp,
            boolean merge) {
        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }
        
        if (possibleConfig.isNothing()) {
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }
        
        Content content;
        try {
            content = complexify(deserialize(new InputStreamReader(req.getInputStream())));
        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }
        
        if (!possibleConfig.requireValue().canWrite(content.getPublisher())) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }
        
        if (Strings.isNullOrEmpty(content.getCanonicalUri())) {
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }

        try {
            content = merge(resolveExisting(content), content, merge);
            if (content instanceof Item) {
                Item item = (Item) content;
                writer.createOrUpdate(item);
                updateSchedule(item);
            } else {
                writer.createOrUpdate((Container) content);
            }
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        }

        String hostName = Configurer.get("local.host.name").get();
        resp.setHeader(
                HttpHeaders.LOCATION,
                hostName + "/3.0/content.json?id=" + codec.encode(BigInteger.valueOf(content.getId()))
        );
        resp.setStatus(HttpStatusCode.OK.code());
        resp.setContentLength(0);
        return null;
    }
    
    private void updateSchedule(Item item) {
        Iterable<Broadcast> broadcasts = Iterables.concat(Iterables.transform(item.getVersions(), Version.TO_BROADCASTS));
        for (Broadcast broadcast : broadcasts) {
            Maybe<Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
            if (channel.hasValue()) {
                scheduleWriter.replaceScheduleBlock(item.getPublisher(), 
                                                    channel.requireValue(), 
                                                    ImmutableSet.of(new ItemRefAndBroadcast(item, broadcast)));
            }
        }        
    }

    private Content merge(Maybe<Identified> possibleExisting, Content update, boolean merge) {
        if (possibleExisting.isNothing()) {
            return update;
        }
        Identified existing = possibleExisting.requireValue();
        if (existing instanceof Content) {
            return merge((Content) existing, update, merge);
        }
        throw new IllegalStateException("Entity for "+update.getCanonicalUri()+" not Content");
    }

    private Content merge(Content existing, Content update, boolean merge) {
        existing.setEquivalentTo(merge ? merge(existing.getEquivalentTo(), update.getEquivalentTo()) : update.getEquivalentTo());
        existing.setLastUpdated(update.getLastUpdated());
        existing.setTitle(update.getTitle());
        existing.setShortDescription(update.getShortDescription());
        existing.setMediumDescription(update.getMediumDescription());
        existing.setLongDescription(update.getLongDescription());
        existing.setDescription(update.getDescription());
        existing.setImage(update.getImage());
        existing.setThumbnail(update.getThumbnail());
        existing.setMediaType(update.getMediaType());
        existing.setSpecialization(update.getSpecialization());
        existing.setRelatedLinks(merge ? merge(existing.getRelatedLinks(), update.getRelatedLinks()) : update.getRelatedLinks());
        existing.setAliases(merge ? merge(existing.getAliases(), update.getAliases()) : update.getAliases());
        existing.setTopicRefs(merge ? merge(existing.getTopicRefs(), update.getTopicRefs()) : update.getTopicRefs());
        existing.setPeople(merge ? merge(existing.people(), update.people()) : update.people());
        existing.setKeyPhrases(update.getKeyPhrases());
        existing.setClips(merge ? merge(existing.getClips(), update.getClips()) : update.getClips());
        existing.setPriority(update.getPriority());
        existing.setEventRefs(merge ? merge(existing.events(), update.events()) : update.events());
        existing.setImages(merge? merge(existing.getImages(), update.getImages()) : update.getImages());
        if (existing instanceof Item && update instanceof Item) {
            return mergeItems((Item)existing, (Item) update);
        }
        return existing;
    }

    private Item mergeItems(Item existing, Item update) {
        if (!update.getVersions().isEmpty()) {
            if (Iterables.isEmpty(existing.getVersions())) {
                existing.addVersion(new Version());
            }
            Version existingVersion = existing.getVersions().iterator().next();
            Version postedVersion = Iterables.getOnlyElement(update.getVersions());
            mergeVersions(existingVersion, postedVersion);
        }
        if (existing instanceof Song && update instanceof Song) {
            return mergeSongs((Song)existing, (Song)update);
        }
        return existing;
    }

    private void mergeVersions(Version existing, Version update) {
        existing.setManifestedAs(update.getManifestedAs());
        existing.setBroadcasts(update.getBroadcasts());
        existing.setSegmentEvents(update.getSegmentEvents());
        existing.setRestriction(update.getRestriction());
    }

    private Song mergeSongs(Song existing, Song update) {
        existing.setIsrc(update.getIsrc());
        existing.setDuration(update.getDuration());
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }

    private <T> List<T> merge(List<T> existing, List<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing)).asList();
    }

    private Maybe<Identified> resolveExisting(Content content) {
        ImmutableSet<String> uris = ImmutableSet.of(content.getCanonicalUri());
        ResolvedContent resolved = resolver.findByCanonicalUris(uris);
        return resolved.get(content.getCanonicalUri());
    }

    private Content complexify(Description inputContent) {
        Content content = transformer.transform(inputContent);
        return updateEventPublisher(content);
    }

    private Description deserialize(Reader input) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), Description.class);
    }
    
    private Void error(HttpServletResponse response, int code) {
        response.setStatus(code);
        response.setContentLength(0);
        return null;
    }

    private Content updateEventPublisher(Content content) {
        List<EventRef> eventRefs = content.events();
        for(EventRef eventRef: eventRefs) {
            Event event = eventResolver.fetch(eventRef.id()).orNull();
            checkNotNull(event);
            checkNotNull(event.publisher());
            eventRef.setPublisher(event.publisher());
        }
        content.setEventRefs(ImmutableList.copyOf(eventRefs));
        return content;
    }
}
