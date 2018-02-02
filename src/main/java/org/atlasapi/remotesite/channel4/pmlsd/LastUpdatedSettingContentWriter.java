package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;
import java.util.Set;

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
import org.atlasapi.reporting.OwlReporter;
import com.metabroadcast.columbus.telescope.api.Event;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.sun.istack.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.NewAlert;

import static com.metabroadcast.status.util.Utils.encode;
import static org.atlasapi.reporting.status.Utils.getPartialStatusForContent;

public class LastUpdatedSettingContentWriter implements ContentWriter {

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final Clock clock;
    private final OwlReporter owlReporter;

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer, Clock clock) {
        this.resolver = resolver;
        this.writer = writer;
        this.clock = clock;
        this.owlReporter = new OwlReporter(OwlTelescopeReporterFactory.getInstance()
                .getTelescopeReporter(OwlTelescopeReporters.CHANNEL_4_INGEST, Event.Type.INGEST));
    }
    
    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer) {
        this(resolver, writer, new SystemClock());
    }
    
    @Override
    public Item createOrUpdate(Item item) {
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


        if (Strings.isNullOrEmpty(item.getTitle())){
            owlReporter.getStatusReporter().updateStatus(
                    EntityRef.Type.CONTENT,
                    item,
                    getPartialStatusForContent(
                            item.getId(),
                            "hhhh",//owlReporter.getTelescopeReporter().getTaskId(),
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
                            "hhhh",
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.TITLE,
                            null,
                            EntityRef.Type.CONTENT,
                            item.getPublisher().key(),
                            true
                    )
            );
        }

        if (item.getGenres() == null || item.getGenres().isEmpty()) {
            owlReporter.getStatusReporter().updateStatus(
                    EntityRef.Type.CONTENT,
                    item,
                    getPartialStatusForContent(
                            item.getId(),
                            "hhhh",
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.GENRE,
                            String.format("Content %s is missing genres.",
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
                            "hhhh",
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.GENRE,
                            null,
                            EntityRef.Type.CONTENT,
                            item.getPublisher().key(),
                            true
                    )
            );
        }

        if (item instanceof Episode)
            if(((Episode) item).getEpisodeNumber() == null) {
            owlReporter.getStatusReporter().updateStatus(
                    EntityRef.Type.CONTENT,
                    item,
                    getPartialStatusForContent(
                            item.getId(),
                            "hhhh",
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.EPISODE_NUMBER,
                            String.format("Content %s is missing an episode number.",
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
                            "hhhh",
                            NewAlert.Key.Check.MISSING,
                            NewAlert.Key.Field.EPISODE_NUMBER,
                            null,
                            EntityRef.Type.CONTENT,
                            item.getPublisher().key(),
                            true
                    )
            );
        }
        
        return writer.createOrUpdate(item);
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
    public void createOrUpdate(Container container) {
        
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
        
        writer.createOrUpdate(container);

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

            if (container.getGenres() == null || container.getGenres().isEmpty()) {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.GENRE,
                                String.format(
                                        "Content %s is missing genres.",
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
                                NewAlert.Key.Field.GENRE,
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

            if (container.getGenres() == null || container.getGenres().isEmpty()) {
                owlReporter.getStatusReporter().updateStatus(
                        EntityRef.Type.CONTENT,
                        container,
                        getPartialStatusForContent(
                                container.getId(),
                                owlReporter.getTelescopeReporter().getTaskId(),
                                NewAlert.Key.Check.MISSING,
                                NewAlert.Key.Field.GENRE,
                                String.format("Content %s is missing genres.",
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
                                NewAlert.Key.Field.GENRE,
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
