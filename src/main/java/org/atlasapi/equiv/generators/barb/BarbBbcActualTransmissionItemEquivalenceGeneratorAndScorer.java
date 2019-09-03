package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
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

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.around;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.expandChannelUris;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasQualifyingBroadcast;

/**
 * This call was created as part of the BBC work to fill in missing PIDs in their CCIDSTxLogs
 * More details can be found in JIRA ENG-339
 */
public class BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private static final Set<Publisher> PUBLISHERS = ImmutableSet.of(Publisher.BBC_NITRO, Publisher.BARB_TRANSMISSIONS);

    // Allow a one second tolerance for the difference between Nitro actual transmission times and txlog broadcast times
    // since txlogs have second level precision but Nitro has millisecond-level precision so we should be lenient around
    // what rounding logic is used.
    private static final Duration ACTUAL_TRANSMISSION_FLEXIBILITY = Duration.standardSeconds(1);

    private static final Set<String> CHANNELS_TO_IGNORE_ACTUAL_TRANSMISSION_END = ImmutableSet.of(
            "http://www.bbc.co.uk/services/bbctwo/wales",
            "http://www.bbc.co.uk/services/bbctwo/ni"
    );

    private final ScheduleResolver resolver;
    private final Duration flexibility;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Score scoreOnMatch;

    public BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Duration flexibility,
            @Nullable Predicate<? super Broadcast> broadcastFilter,
            Score scoreOnMatch
    ) {
        this.resolver = checkNotNull(resolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.flexibility = checkNotNull(flexibility);
        this.broadcastFilter = broadcastFilter == null ? broadcast -> true : broadcastFilter;
        this.scoreOnMatch = checkNotNull(scoreOnMatch);
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        checkArgument(PUBLISHERS.contains(subject.getPublisher()));

        Builder<Item> scores = DefaultScoredCandidates.fromSource("BARB-BBC Actual Transmission");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB-BBC Actual Transmission Item Equivalence Generator");


        Set<Publisher> validPublishers = Sets.difference(
                PUBLISHERS,
                ImmutableSet.of(subject.getPublisher())
        );

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        for (Version version : subject.getVersions()) {
            int broadcastCount = version.getBroadcasts().size();
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (broadcast.isActivelyPublished()
                        && broadcastFilter.test(broadcast)
                ) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            subject,
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
                PUBLISHERS
        );
    }

    private void findMatchesForBroadcast(
            Item subject,
            Builder<Item> scores,
            Broadcast subjectBroadcast,
            Set<Publisher> validPublishers,
            EquivToTelescopeComponent generatorComponent
    ) {
        Set<String> channelUris = expandChannelUris(subjectBroadcast.getBroadcastOn());
        Schedule schedule = scheduleAround(subjectBroadcast, channelUris, validPublishers);

        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            for (Item scheduleItem : channel.items()) {
                if (scheduleItem.isActivelyPublished()
                        && hasQualifyingBroadcast(scheduleItem, subjectBroadcast, flexibility)
                        && hasQualifyingActualTransmissionTimeBroadcast(subject, scheduleItem, subjectBroadcast)
                ) {
                    scores.updateEquivalent(scheduleItem, scoreOnMatch);

                    if (scheduleItem.getId() != null) {
                        generatorComponent.addComponentResult(scheduleItem.getId(), scoreOnMatch.toString());
                    }

                }
            }
        }
    }

    private boolean hasQualifyingActualTransmissionTimeBroadcast(
            Item subject,
            Item scheduleItem,
            Broadcast subjectBroadcast
    ) {
        checkArgument(
                PUBLISHERS.contains(subject.getPublisher())
                        && PUBLISHERS.contains(scheduleItem.getPublisher())
                        && subject.getPublisher() != scheduleItem.getPublisher()
        );
        for (Version scheduleVersion : scheduleItem.nativeVersions()) {
            for (Broadcast scheduleBroadcast : scheduleVersion.getBroadcasts()) {
                if (subject.getPublisher() == Publisher.BARB_TRANSMISSIONS
                        && scheduleItem.getPublisher() == Publisher.BBC_NITRO
                        && hasQualifyingActualTransmissionTime(subjectBroadcast, scheduleBroadcast)
                ) {
                    return true;
                } else if (subject.getPublisher() == Publisher.BBC_NITRO
                        && scheduleItem.getPublisher() == Publisher.BARB_TRANSMISSIONS
                        && hasQualifyingActualTransmissionTime(scheduleBroadcast, subjectBroadcast)
                ) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasQualifyingActualTransmissionTime(Broadcast txlogBroadcast, Broadcast nitroBroadcast) {
        // Actual end time is generally missing from these channels so BBC have requested us
        // to only check the actual start time for these
        boolean onlyCheckActualStartTime = CHANNELS_TO_IGNORE_ACTUAL_TRANSMISSION_END
                .contains(nitroBroadcast.getBroadcastOn());
        if (nitroBroadcast.getActualTransmissionTime() == null
                || (nitroBroadcast.getActualTransmissionEndTime() == null && !onlyCheckActualStartTime)
        ) {
            return false;
        }
        return around(
                txlogBroadcast.getTransmissionTime(),
                nitroBroadcast.getActualTransmissionTime(),
                ACTUAL_TRANSMISSION_FLEXIBILITY
        ) && (onlyCheckActualStartTime
                || around(
                txlogBroadcast.getTransmissionEndTime(),
                nitroBroadcast.getActualTransmissionEndTime(),
                ACTUAL_TRANSMISSION_FLEXIBILITY
        )
        );
    }


    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime().minus(flexibility);
        DateTime end = broadcast.getTransmissionEndTime().plus(flexibility);

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
        return "BARB-BBC Actual Transmission Item Equivalence Generator";
    }
}
