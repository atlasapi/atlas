package org.atlasapi.query.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteExecutor {

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ScheduleWriter scheduleWriter;
    private final ChannelResolver channelResolver;
    private final EventResolver eventResolver;

    public ContentWriteExecutor(
            ContentResolver resolver,
            ContentWriter writer,
            ScheduleWriter scheduleWriter,
            ChannelResolver channelResolver,
            EventResolver eventResolver
    ) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.channelResolver = checkNotNull(channelResolver);
        this.eventResolver = checkNotNull(eventResolver);
    }

    public Long writeContent(Content content, String type, boolean shouldMerge) {
        Content updatedContent = updateEventPublisher(content);

        Maybe<Identified> identified = resolveExisting(updatedContent);

        if (type.equals("broadcast")) {
            updatedContent = mergeBroadcasts(identified, updatedContent);
        } else {
            updatedContent = merge(identified, updatedContent, shouldMerge);
        }
        if (updatedContent instanceof Item) {
            Item item = (Item) updatedContent;
            writer.createOrUpdate(item);
            updateSchedule(item);
        } else {
            writer.createOrUpdate((Container) updatedContent);
        }
        return updatedContent.getId();
    }

    private Content updateEventPublisher(Content content) {
        List<EventRef> eventRefs = content.events();
        for (EventRef eventRef : eventRefs) {
            Event event = eventResolver.fetch(eventRef.id()).orNull();
            checkNotNull(event);
            checkNotNull(event.publisher());
            eventRef.setPublisher(event.publisher());
        }
        content.setEventRefs(ImmutableList.copyOf(eventRefs));
        return content;
    }

    private Maybe<Identified> resolveExisting(Content content) {
        ImmutableSet<String> uris = ImmutableSet.of(content.getCanonicalUri());
        ResolvedContent resolved = resolver.findByCanonicalUris(uris);
        return resolved.get(content.getCanonicalUri());
    }

    private Content mergeBroadcasts(Maybe<Identified> possibleExisting, Content update) {
        if (possibleExisting.isNothing()) {
            return update;
        }
        Identified existing = possibleExisting.requireValue();
        if (!(existing instanceof Item)) {
            throw new IllegalStateException("Entity for "
                    + update.getCanonicalUri()
                    + " not Content");
        }
        Item item = (Item) existing;
        if (!update.getVersions().isEmpty()) {
            if (Iterables.isEmpty(item.getVersions())) {
                item.addVersion(new Version());
            }
            Version existingVersion = item.getVersions().iterator().next();
            Version postedVersion = Iterables.getOnlyElement(update.getVersions());
            mergeVersions(existingVersion, postedVersion);
        }
        return (Content) existing;
    }

    private Content merge(Maybe<Identified> possibleExisting, Content update, boolean merge) {
        if (possibleExisting.isNothing()) {
            return update;
        }
        Identified existing = possibleExisting.requireValue();
        if (existing instanceof Content) {
            return merge((Content) existing, update, merge);
        }
        throw new IllegalStateException("Entity for " + update.getCanonicalUri() + " not Content");
    }

    private Content merge(Content existing, Content update, boolean merge) {
        existing.setEquivalentTo(merge ?
                                 merge(existing.getEquivalentTo(), update.getEquivalentTo()) :
                                 update.getEquivalentTo());
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
        existing.setRelatedLinks(merge ?
                                 merge(existing.getRelatedLinks(), update.getRelatedLinks()) :
                                 update.getRelatedLinks());
        existing.setAliases(merge ?
                            merge(existing.getAliases(), update.getAliases()) :
                            update.getAliases());
        existing.setTopicRefs(merge ?
                              merge(existing.getTopicRefs(), update.getTopicRefs()) :
                              update.getTopicRefs());
        existing.setPeople(merge ? merge(existing.people(), update.people()) : update.people());
        existing.setKeyPhrases(update.getKeyPhrases());
        existing.setClips(merge ?
                          merge(existing.getClips(), update.getClips()) :
                          update.getClips());
        existing.setPriority(update.getPriority());
        existing.setEventRefs(merge ? merge(existing.events(), update.events()) : update.events());
        existing.setImages(merge ?
                           merge(existing.getImages(), update.getImages()) :
                           update.getImages());

        if (existing instanceof Episode && update instanceof Episode) {
            mergeEpisodes((Episode) existing, (Episode) update);
        }
        if (existing instanceof Item && update instanceof Item) {
            return mergeItems((Item) existing, (Item) update);
        }
        return existing;
    }

    private Item mergeEpisodes(Episode existing, Episode update) {
        existing.setSeriesNumber(update.getSeriesNumber());
        existing.setEpisodeNumber(update.getEpisodeNumber());
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
        existing.setCountriesOfOrigin(update.getCountriesOfOrigin());
        existing.setYear(update.getYear());
        if (existing instanceof Song && update instanceof Song) {
            return mergeSongs((Song) existing, (Song) update);
        }
        return existing;
    }

    private void mergeVersions(Version existing, Version update) {
        existing.setDuration(Duration.standardSeconds(update.getDuration().longValue()));
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

    private void updateSchedule(Item item) {
        Iterable<Broadcast> broadcasts = Iterables.concat(Iterables.transform(item.getVersions(),
                Version.TO_BROADCASTS));
        for (Broadcast broadcast : broadcasts) {
            Maybe<Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
            if (channel.hasValue()) {
                scheduleWriter.replaceScheduleBlock(item.getPublisher(),
                        channel.requireValue(),
                        ImmutableSet.of(new ScheduleEntry.ItemRefAndBroadcast(item, broadcast)));
            }
        }
    }
}
