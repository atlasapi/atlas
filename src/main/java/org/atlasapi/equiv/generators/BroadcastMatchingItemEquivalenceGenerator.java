package org.atlasapi.equiv.generators;

import java.util.Set;

import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class BroadcastMatchingItemEquivalenceGenerator implements EquivalenceGenerator<Item>{

    private final ScheduleResolver resolver;
    private final Set<Publisher> supportedPublishers;
    private final Duration flexibility;
	private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> filter;
    private final Duration EXTENDED_END_TIME_FLEXIBILITY = Duration
            .standardHours(3)
            .plus(Duration.standardMinutes(5));

    public BroadcastMatchingItemEquivalenceGenerator(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers,
            Duration flexibility,
            Predicate<? super Broadcast> filter
    ) {
        this.resolver = resolver;
        this.channelResolver = channelResolver;
        this.supportedPublishers = supportedPublishers;
        this.flexibility = flexibility;
        this.filter = filter;
    }
    
    public BroadcastMatchingItemEquivalenceGenerator(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers,
            Duration flexibility
    ) {
        this(
                resolver,
                channelResolver,
                supportedPublishers,
                flexibility,
                new Predicate<Broadcast>() {
            @Override
            public boolean apply(Broadcast input) {
                DateTime eightDaysInFuture = new DateTime(DateTimeZones.UTC).plusDays(8);
                return input.getTransmissionTime().isBefore(eightDaysInFuture);
            }
        });
    }
    
    public BroadcastMatchingItemEquivalenceGenerator(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers
    ) {
        this(resolver, channelResolver, supportedPublishers, Duration.standardMinutes(5));
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item content,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {

        Builder<Item> scores = DefaultScoredCandidates.fromSource("broadcast");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Broadcast Matching Item Equivalence Generator");

        Set<Publisher> validPublishers = Sets.difference(
                supportedPublishers,
                ImmutableSet.of(content.getPublisher())
        );

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        for (Version version : content.getVersions()) {
            int broadcastCount = version.getBroadcasts().size();
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (broadcast.isActivelyPublished() 
                        && (!onIgnoredChannel(broadcast) || broadcastCount == 1) 
                        && filter.apply(broadcast)) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            scores,
                            broadcast,
                            validPublishers,
                            generatorComponent
                    );
                }
            }
        }

        equivToTelescopeResults.addGeneratorResult(generatorComponent);

        desc.appendText("Processed %s of %s broadcasts", processedBroadcasts, totalBroadcasts);

        return scale(scores.build(), processedBroadcasts, desc);
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                supportedPublishers
        );
    }

    private void findMatchesForBroadcast(
            Builder<Item> scores,
            Broadcast broadcast,
            Set<Publisher> validPublishers,
            EquivToTelescopeComponent generatorComponent
    ) {

        Schedule schedule = scheduleAround(broadcast, validPublishers);

        if (schedule == null) {
            return;
        }
        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            for (Item scheduleItem : channel.items()) {
                if (scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasQualifyingBroadcast(scheduleItem, broadcast)) {
                    scores.addEquivalent(scheduleItem, Score.valueOf(1.0));

                    generatorComponent.addComponentResult(scheduleItem.getId(), "1.0");

                } else if (scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasFlexibleQualifyingBroadcast(scheduleItem, broadcast)) {
                    scores.addEquivalent(scheduleItem, Score.valueOf(0.1));

                    generatorComponent.addComponentResult(scheduleItem.getId(), "0.1");
                }
            }
        }
    }

    private boolean onIgnoredChannel(Broadcast broadcast) {
        return ignoredChannels.contains(broadcast.getBroadcastOn());
    }

    private static final Set<String> ignoredChannels = ImmutableSet.<String>builder()
		.add("http://www.bbc.co.uk/services/bbcone/ni")
		.add("http://www.bbc.co.uk/services/bbcone/cambridge")
		.add("http://www.bbc.co.uk/services/bbcone/channel_islands")
		.add("http://www.bbc.co.uk/services/bbcone/east")
		.add("http://www.bbc.co.uk/services/bbcone/east_midlands")
		.add("http://www.bbc.co.uk/services/bbcone/hd")
		.add("http://www.bbc.co.uk/services/bbcone/north_east")
		.add("http://www.bbc.co.uk/services/bbcone/north_west")
		.add("http://www.bbc.co.uk/services/bbcone/oxford")
		.add("http://www.bbc.co.uk/services/bbcone/scotland")
		.add("http://www.bbc.co.uk/services/bbcone/south")
		.add("http://www.bbc.co.uk/services/bbcone/south_east")
		.add("http://www.bbc.co.uk/services/bbcone/wales")
		.add("http://www.bbc.co.uk/services/bbcone/south_west")
		.add("http://www.bbc.co.uk/services/bbcone/west")
		.add("http://www.bbc.co.uk/services/bbcone/west_midlands")
		.add("http://www.bbc.co.uk/services/bbcone/east_yorkshire")
		.add("http://www.bbc.co.uk/services/bbcone/yorkshire")
		.add("http://www.bbc.co.uk/services/bbctwo/ni")
		.add("http://www.bbc.co.uk/services/bbctwo/ni_analogue")
		.add("http://www.bbc.co.uk/services/bbctwo/scotland")
		.add("http://www.bbc.co.uk/services/bbctwo/wales")
		.add("http://www.bbc.co.uk/services/bbctwo/wales_analogue")
		.add("http://www.bbc.co.uk/services/radio4/lw")
     .build();

    private ScoredCandidates<Item> scale(
            ScoredCandidates<Item> scores,
            final int broadcasts,
            final ResultDescription desc
    ) {
        return DefaultScoredCandidates.fromMappedEquivs(
                scores.source(),
                Maps.transformEntries(scores.candidates(),
                        new EntryTransformer<Item, Score, Score>() {
            @Override
            public Score transformEntry(Item key, Score value) {
                desc.appendText("%s matched %s broadcasts", key.getCanonicalUri(), value);
                return value.transform(new Function<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        return input / broadcasts;
                    }
                });
            }
        }));
    }

    private boolean hasQualifyingBroadcast(Item item, Broadcast referenceBroadcast) {
        for (Version version : item.nativeVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (around(broadcast, referenceBroadcast) && broadcast.getBroadcastOn() != null
                        && broadcast.getBroadcastOn().equals(referenceBroadcast.getBroadcastOn())
                        && broadcast.isActivelyPublished()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasFlexibleQualifyingBroadcast(Item item, Broadcast referenceBroadcast) {
        for (Version version : item.nativeVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (flexibleAround(broadcast, referenceBroadcast)
                        && broadcast.getBroadcastOn() != null
                        && broadcast.getBroadcastOn().equals(referenceBroadcast.getBroadcastOn())
                        && broadcast.isActivelyPublished()) {
                    return true;
                }
            }
        }
        return false;
    }


    private boolean around(Broadcast broadcast, Broadcast referenceBroadcast) {
        return around(broadcast.getTransmissionTime(), referenceBroadcast.getTransmissionTime())
            && around(broadcast.getTransmissionEndTime(), referenceBroadcast.getTransmissionEndTime());
    }

    private boolean around(DateTime transmissionTime, DateTime transmissionTime2) {
        return !transmissionTime.isBefore(transmissionTime2.minus(flexibility))
            && !transmissionTime.isAfter(transmissionTime2.plus(flexibility));
    }

    private boolean flexibleAround(Broadcast broadcast, Broadcast referenceBroadcast) {
        return around(broadcast.getTransmissionTime(), referenceBroadcast.getTransmissionTime())
                && flexibleAroundEndTime(broadcast.getTransmissionEndTime(),
                referenceBroadcast.getTransmissionEndTime());
    }

    private boolean flexibleAroundEndTime(DateTime transmissionTime, DateTime transmissionTime2) {
        return !transmissionTime.isBefore(transmissionTime2.minus(EXTENDED_END_TIME_FLEXIBILITY))
                && !transmissionTime.isAfter(transmissionTime2.plus(EXTENDED_END_TIME_FLEXIBILITY));
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime().minus(flexibility);
        DateTime end = broadcast.getTransmissionEndTime().plus(flexibility);
        Maybe<Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
        if (channel.hasValue()) {
            return resolver.unmergedSchedule(
                    start,
                    end,
                    ImmutableSet.of(channel.requireValue()),
                    publishers
            );
        }
        return null;
    }
    
    @Override
    public String toString() {
        return "Broadcast-matching generator";
    }
}
