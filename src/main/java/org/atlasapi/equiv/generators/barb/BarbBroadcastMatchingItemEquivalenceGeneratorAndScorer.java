package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
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

/**
 * This is mostly a copy of {@link BroadcastMatchingItemEquivalenceGeneratorAndScorer} class but with some logic
 * stripped out. For certain channels it also generates candidates from additional channels.
 */
public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    /**
     * There may be a solution for this that utilises channel equivalence however due to time constraints and a lack of
     * knowledge of channel equivalence I have opted to use a hardcoded map for the time being.
     * <p>
     * N.B. that the MAS file ingest already uses channel equivalence to produce the station-codes.tsv file and so if
     * any other non-barb channel is equived to the barb channel its uri is output in the tsv file and all txlogs for
     * that station code will be ingested on the non-barb channel instead. In the case of txlog BBC2 England regional
     * channels this meant they were all originally being ingested on the single Nitro channel causing most of their
     * broadcasts to become unpublished since the channel can only have one piece of content present in a given time
     * slot. Now they are ingested on their own barb channels and use this map in order to search for candidates from
     * the Nitro channel. If changing to using channel equivalence for candidate generation then the MAS file ingest may
     * need to be changed to make sure broadcasts on txlog content are ingested correctly.
     */
    public static final ImmutableSetMultimap<String, String> CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS =
            ImmutableSetMultimap.<String, String>builder()
                    .putAll(
                            "http://www.bbc.co.uk/services/bbctwo/england", ImmutableSet.of(
                                    "http://channels.barb.co.uk/channels/1081",
                                    "http://channels.barb.co.uk/channels/1082",
                                    "http://channels.barb.co.uk/channels/1083",
                                    "http://channels.barb.co.uk/channels/1084",
                                    "http://channels.barb.co.uk/channels/1085",
                                    "http://channels.barb.co.uk/channels/1086",
                                    "http://channels.barb.co.uk/channels/1087",
                                    "http://channels.barb.co.uk/channels/1088",
                                    "http://channels.barb.co.uk/channels/1093",
                                    "http://channels.barb.co.uk/channels/1094",
                                    "http://channels.barb.co.uk/channels/1095"
                            )
                    )
                    .build();

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
                        && hasQualifyingBroadcast(scheduleItem, broadcast)) {
                    scores.addEquivalent(scheduleItem, scoreOnMatch);

                    if (scheduleItem.getId() != null) {
                        generatorComponent.addComponentResult(
                                scheduleItem.getId(),
                                scoreOnMatch.toString()
                        );
                    }

                } else if (scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasFlexibleQualifyingBroadcast(scheduleItem, broadcast)) {
                    scores.addEquivalent(scheduleItem, scoreOnExtendedFlexibilityMatch);

                    generatorComponent.addComponentResult(
                            scheduleItem.getId(),
                            scoreOnExtendedFlexibilityMatch.toString()
                    );
                }
            }
        }
    }

    private Set<String> expandChannelUris(String channelUri) {
        ImmutableSet.Builder<String> channelUris = ImmutableSet.builder();
        channelUris.add(channelUri);
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(channelUri)) {
            channelUris.addAll(CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(channelUri));
        } else if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsValue(channelUri)) {
            channelUris.addAll(CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.inverse().get(channelUri));
        }
        return channelUris.build();
    }

    private boolean hasQualifyingBroadcast(Item item, Broadcast referenceBroadcast) {
        for (Version version : item.nativeVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (around(broadcast, referenceBroadcast) && broadcast.getBroadcastOn() != null
                        && sameChannel(broadcast.getBroadcastOn(), referenceBroadcast.getBroadcastOn())
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
                        && sameChannel(broadcast.getBroadcastOn(), referenceBroadcast.getBroadcastOn())
                        && broadcast.isActivelyPublished()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean sameChannel(String channelUri, String otherChannelUri) {
        if (channelUri.equals(otherChannelUri)) {
            return true;
        }
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(channelUri)) {
            if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(channelUri).contains(otherChannelUri)) {
                return true;
            }
        }
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(otherChannelUri)) {
            if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(otherChannelUri).contains(channelUri)) {
                return true;
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
