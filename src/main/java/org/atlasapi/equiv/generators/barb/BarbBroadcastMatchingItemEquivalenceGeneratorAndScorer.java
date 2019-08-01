package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.expandChannelUris;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasFlexibleQualifyingBroadcast;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasQualifyingBroadcast;

/**
 * This is mostly a copy of {@link BroadcastMatchingItemEquivalenceGeneratorAndScorer} class but with some logic
 * stripped out. For certain channels it also generates candidates from additional channels.
 */
public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private final ScheduleResolver resolver;
    private final Set<Publisher> supportedPublishers;
    private final Duration flexibility;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Duration EXTENDED_END_TIME_FLEXIBILITY = Duration
            .standardHours(3)
            .plus(Duration.standardMinutes(5));
    private final Duration SHORT_CONTENT_REDUCED_TIME_FLEXIBILITY = Duration.standardMinutes(10);
    private final Score scoreOnMatch;
    private final Score scoreOnExtendedFlexibilityMatch;

    public BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers,
            Duration flexibility,
            Predicate<? super Broadcast> broadcastFilter,
            Score scoreOnMatch,
            Score scoreOnExtendedFlexibilityMatch
    ) {
        this.resolver = checkNotNull(resolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.supportedPublishers = checkNotNull(supportedPublishers);
        this.flexibility = checkNotNull(flexibility);
        this.broadcastFilter = broadcastFilter == null ? broadcast -> true : broadcastFilter;
        this.scoreOnMatch = checkNotNull(scoreOnMatch);
        this.scoreOnExtendedFlexibilityMatch = checkNotNull(scoreOnExtendedFlexibilityMatch);
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {

        Builder<Item> scores = DefaultScoredCandidates.fromSource("BARB Broadcast");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB Broadcast Matching Item Equivalence Generator");

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
                if (broadcast.isActivelyPublished() && broadcastFilter.test(broadcast)) {
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

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        desc.appendText("Processed %s of %s broadcasts", processedBroadcasts, totalBroadcasts);

        return scores.build();
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
        Set<String> channelUris = expandChannelUris(broadcast.getBroadcastOn());
        Schedule schedule = scheduleAround(broadcast, channelUris, validPublishers);
        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            for (Item scheduleItem : channel.items()) {
                if (scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasQualifyingBroadcast(scheduleItem, broadcast, flexibility)) {
                    scores.addEquivalent(scheduleItem, scoreOnMatch);

                    if (scheduleItem.getId() != null) {
                        generatorComponent.addComponentResult(
                                scheduleItem.getId(),
                                scoreOnMatch.toString()
                        );
                    }

                } else if (scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasFlexibleQualifyingBroadcast(
                                scheduleItem, broadcast, flexibility, EXTENDED_END_TIME_FLEXIBILITY
                )) {
                    scores.addEquivalent(scheduleItem, scoreOnExtendedFlexibilityMatch);

                    generatorComponent.addComponentResult(
                            scheduleItem.getId(),
                            scoreOnExtendedFlexibilityMatch.toString()
                    );
                }
            }
        }
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        Duration shortBroadcastFlexibility = Duration.standardMinutes(2);
        Duration broadcastPeriod = new Duration(
                broadcast.getTransmissionTime(),
                broadcast.getTransmissionEndTime()
        );

        DateTime start = broadcast.getTransmissionTime().minus(flexibility);
        DateTime end = broadcast.getTransmissionEndTime().plus(flexibility);

        // if the broadcast is less than 10 minutes long, reduce the flexibility
        if (broadcastPeriod.compareTo(SHORT_CONTENT_REDUCED_TIME_FLEXIBILITY) < 0) {
            start = broadcast.getTransmissionTime().minus(shortBroadcastFlexibility);
            end = broadcast.getTransmissionEndTime().plus(shortBroadcastFlexibility);
        }
        Set<Channel> channels = channelUris.parallelStream()
                .map(channelResolver::fromUri)
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .collect(MoreCollectors.toImmutableSet());
        return resolver.unmergedSchedule(
                start,
                end,
                channels,
                publishers
        );
    }

    @Override
    public String toString() {
        return "BARB Broadcast-matching generator";
    }
}
