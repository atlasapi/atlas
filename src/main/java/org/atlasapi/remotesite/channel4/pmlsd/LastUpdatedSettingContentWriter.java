package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.NewAlert;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.sparql.pfunction.library.container;
import com.sun.syndication.feed.atom.Entry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.metabroadcast.status.util.Utils.encode;
import static org.atlasapi.reporting.status.Utils.getPartialStatusForContent;

public class LastUpdatedSettingContentWriter implements C4ContentWriter {

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final Clock clock;
    private final OwlReporter owlReporter;

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer, Clock clock, OwlReporter owlReporter) {
        this.resolver = resolver;
        this.writer = writer;
        this.clock = clock;
        this.owlReporter = owlReporter;
    }
    
    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer, OwlReporter owlReporter) {
        this(resolver, writer, new SystemClock(), owlReporter);
    }

    @Override
    public Item createOrUpdate(Item item, @Nullable Object entry) {

        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(item.getCanonicalUri())).get(item.getCanonicalUri());

        DateTime now = clock.now();
        if(previously.hasValue() && previously.requireValue() instanceof Item) {
            Item prevItem = (Item) previously.requireValue();
            if(!equal(prevItem, item)){
                item.setLastUpdated(now);
            }
            setUpdatedVersions(prevItem.getVersions(), item.getVersions(), now);
        }
        else {
            setUpdatedVersions(Sets.<Version>newHashSet(), item.getVersions(), now);
        }

        if(item.getLastUpdated() == null  || previously.isNothing()) {
            item.setLastUpdated(clock.now());
        }

        try {
            item = writer.createOrUpdate(item); // do this first, as we need the ID for new content

            owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                    item.getId(),
                    item.getAliases(),
                    EntityType.CONTENT,
                    entry
            );
        } catch (Exception e) {
            owlReporter.getTelescopeReporter().reportFailedEvent(
                    item.getId(),
                    String.format("Failed to create or update item %s.",
                            encode(item.getId())
                    ),
                    EntityType.CONTENT,
                    entry
            );
        }

        if (Strings.isNullOrEmpty(item.getTitle())){
            owlReporter.getStatusReporter().updateStatus(
                    EntityRef.Type.CONTENT,
                    item,
                    getPartialStatusForContent(
                            item.getId(),
                            owlReporter.getTelescopeReporter().getTaskId(),
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.TITLE,
                            String.format("Content %s is missing a title.",
                                    encode(item.getId())
                            ),
                            EntityRef.Type.CONTENT,
                            item.getPublisher().key(),
                            false
                    )
            );
        } else {
            owlReporter.getStatusReporter().updateStatus(
                    EntityRef.Type.CONTENT,
                    item,
                    getPartialStatusForContent(
                            item.getId(),
                            owlReporter.getTelescopeReporter().getTaskId(),
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.TITLE,
                            null,
                            EntityRef.Type.CONTENT,
                            item.getPublisher().key(),
                            true
                    )
            );
        }

        if (item instanceof Episode) {
            if (((Episode) item).getEpisodeNumber() == null) {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        item,
                        getPartialStatusForContent(
                                item.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.EPISODE_NUMBER,
                                String.format(
                                        "Content %s is missing an episode number.",
                                        encode(item.getId())
                                ),
                                EntityRef.Type.CONTENT,
                                item.getPublisher().key(),
                                false
                        )
                );
            } else {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        item,
                        getPartialStatusForContent(
                                item.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.EPISODE_NUMBER,
                                null,
                                EntityRef.Type.CONTENT,
                                item.getPublisher().key(),
                                true
                        )
                );
            }
        }

        return item;
    }

    private void setUpdatedVersions(Set<Version> prevVersions, Set<Version> versions, DateTime now) {
        
        Map<String, Broadcast> prevBroadcasts = previousBroadcasts(prevVersions);

        for (Version version : versions) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                Broadcast prevBroadcast = prevBroadcasts.get(broadcast.getSourceId());
                if (prevBroadcast == null || !equal(prevBroadcast, broadcast)) {
                    broadcast.setLastUpdated(now);
                }
            }

            for (Encoding encoding : version.getManifestedAs()) {
                for (Location location : encoding.getAvailableAt()) {
                    Location prevLocation = findPreviousLocation(prevVersions, location);

                    if (prevLocation != null) {
                        location.setLastUpdated(now);
                    }
                }
            }
        }

    }

    @Nullable
    private Location findPreviousLocation(Set<Version> prevVersions, Location location) {
        return prevVersions.stream()
                .flatMap(v -> v.getManifestedAs().stream())
                .flatMap(e -> e.getAvailableAt().stream())
                .filter(prevLocation -> equal(prevLocation, location))
                .findFirst()
                .orElse(null);
    }

    private boolean equal(Location prevLocation, Location location) {
        return equal(prevLocation.getPolicy(), location.getPolicy());
    }

    private boolean equal(Policy prevPolicy, Policy policy) {
        return Objects.equal(prevPolicy.getAvailabilityStart().toDateTime(DateTimeZone.UTC), policy.getAvailabilityStart().toDateTime(DateTimeZone.UTC))
            && Objects.equal(prevPolicy.getAvailabilityEnd().toDateTime(DateTimeZone.UTC), policy.getAvailabilityEnd().toDateTime(DateTimeZone.UTC))
            && Objects.equal(prevPolicy.getAvailableCountries(), policy.getAvailableCountries());
    }

    private boolean equal(Broadcast prevBroadcast, Broadcast broadcast) {
        return Objects.equal(prevBroadcast.getTransmissionTime().toDateTime(DateTimeZone.UTC),broadcast.getTransmissionTime().toDateTime(DateTimeZone.UTC))
            && Objects.equal(prevBroadcast.getTransmissionEndTime().toDateTime(DateTimeZone.UTC), broadcast.getTransmissionEndTime().toDateTime(DateTimeZone.UTC))
            && Objects.equal(prevBroadcast.getBroadcastDuration(), broadcast.getBroadcastDuration())
            && Objects.equal(prevBroadcast.isActivelyPublished(), broadcast.isActivelyPublished());
    }

    private ImmutableMap<String, Broadcast> previousBroadcasts(Set<Version> prevVersions) {
        Iterable<Broadcast> allBroadcasts = Iterables.concat(Iterables.transform(prevVersions, new Function<Version, Iterable<Broadcast>>() {
            @Override
            public Iterable<Broadcast> apply(Version input) {
                return input.getBroadcasts();
            }
        }));
        return Maps.uniqueIndex(allBroadcasts, new Function<Broadcast, String>() {

            @Override
            public String apply(Broadcast input) {
                return input.getSourceId();
            }
        });
    }

    private boolean equal(Item prevItem, Item item) {
        return Objects.equal(item.getDescription(), prevItem.getDescription())
            && Objects.equal(item.getGenres(), prevItem.getGenres())
            && Objects.equal(item.getImage(), prevItem.getImage())
            && Objects.equal(item.getThumbnail(), prevItem.getThumbnail())
            && Objects.equal(item.getTitle(), prevItem.getTitle());
        
    }

    @Override
    public void createOrUpdate(Container container, @Nullable Object entry) {

        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(container.getCanonicalUri())).get(container.getCanonicalUri());
        
        if(previously.hasValue() && previously.requireValue() instanceof Container) {
            Container prevContainer = (Container) previously.requireValue();
            if(!equal(prevContainer, container)) {
                container.setLastUpdated(clock.now());
                container.setThisOrChildLastUpdated(clock.now());
            }
        }
        
        if(container.getLastUpdated() == null || previously.isNothing()) {
            container.setLastUpdated(clock.now());
            container.setThisOrChildLastUpdated(clock.now());
        }

        try {
            writer.createOrUpdate(container);

            owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                    container.getId(),
                    container.getAliases(),
                    EntityType.CONTENT,
                    entry
            );
        } catch (Exception e) {
            owlReporter.getTelescopeReporter().reportFailedEvent(
                    container.getId(),
                    String.format("Failed to create or update item %s.",
                            encode(container.getId())
                    ),
                    EntityType.CONTENT,
                    entry
            );
        }

        if (container instanceof Series) {
            if (Strings.isNullOrEmpty(container.getTitle())) {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE,
                                String.format(
                                        "Content %s is missing a title.",
                                        encode(container.getId())
                                ),
                                EntityRef.Type.CONTENT,
                                container.getPublisher().key(),
                                false
                        )
                );
            } else {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE,
                                null,
                                EntityRef.Type.CONTENT,
                                container.getPublisher().key(),
                                true
                        )
                );
            }
        }
        else if(container instanceof Brand){
            if (Strings.isNullOrEmpty(container.getTitle())) {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE,
                                String.format("Content %s is missing a title.",
                                        encode(container.getId())
                                ),
                                EntityRef.Type.CONTENT,
                                container.getPublisher().key(),
                                false
                        )
                );
            } else {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.TITLE,
                                null,
                                EntityRef.Type.CONTENT,
                                container.getPublisher().key(),
                                true
                        )
                );
            }
        }
    }

    private boolean equal(Container prevContainer, Container container) {
        return Objects.equal(prevContainer.getAliasUrls(), container.getAliasUrls())
            && Objects.equal(prevContainer.getTitle(), container.getTitle())
            && Objects.equal(prevContainer.getDescription(), container.getDescription());
    }

}
