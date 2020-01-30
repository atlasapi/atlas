package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
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
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.BBC1_TXLOG_CHANNEL_URIS;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.BBC2_TXLOG_CHANNEL_URIS;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.around;

/**
 * Equiv BBC Txlog entries accross regions if start time, end time and title are all exactly the same.
 * This is further additional work for BBC network linking.
 */
public class BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private static final Set<Publisher> ALLOWED_PUBLISHERS = ImmutableSet.of(
            Publisher.LAYER3_TXLOGS,
            Publisher.BARB_TRANSMISSIONS
    );

    private final ScheduleResolver scheduleResolver;
    private final Set<Publisher> publishers;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Duration flexibility;
    private final Score scoreOnMatch;

    private BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer(Builder builder) {
        this.scheduleResolver = checkNotNull(builder.scheduleResolver);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.publishers = ImmutableSet.copyOf(builder.publishers);
        checkArgument(ALLOWED_PUBLISHERS.containsAll(this.publishers));
        this.broadcastFilter = builder.broadcastFilter == null ? broadcast -> true : builder.broadcastFilter;
        this.flexibility = checkNotNull(builder.flexibility);
        this.scoreOnMatch = checkNotNull(builder.scoreOnMatch);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        checkArgument(ALLOWED_PUBLISHERS.contains(subject.getPublisher()));

        DefaultScoredCandidates.Builder<Item> scores =
                DefaultScoredCandidates.fromSource("BARB-BBC Regional Txlog Generator");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB-BBC Regional Txlog Generator");

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        for (Version version : subject.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (broadcast.isActivelyPublished() && broadcastFilter.test(broadcast)) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            subject,
                            scores,
                            broadcast,
                            publishers,
                            desc,
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
                publishers
        );
    }

    private void findMatchesForBroadcast(
            Item subject,
            DefaultScoredCandidates.Builder<Item> scores,
            Broadcast subjectBroadcast,
            Set<Publisher> publishers,
            ResultDescription desc,
            EquivToTelescopeComponent generatorComponent
    ) {
        Set<String> channelUris = expandRegionalChannelUris(subjectBroadcast.getBroadcastOn());
        if (channelUris == null) {
            return;
        }

        desc.startStage(
                "Finding matches for broadcast: " + String.format(
                        "%s [%s - %s]",
                        subjectBroadcast.getBroadcastOn(),
                        subjectBroadcast.getTransmissionTime(),
                        subjectBroadcast.getTransmissionEndTime())
        );

        Schedule schedule = scheduleAround(subjectBroadcast, channelUris, publishers);

        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            ListMultimap<Publisher, Item> publisherItems = filterScheduleByPublisher(channel);

            for (Publisher publisher : publisherItems.keySet()) {
                desc.startStage(
                        "Candidate schedule found for " + publisher.key()
                                + " for " + channel.channel().getUri()
                );
                for (Item scheduleItem : channel.items()) {
                    if (!scheduleItem.isActivelyPublished()
                            || subject.getCanonicalUri().equals(scheduleItem.getCanonicalUri())
                    ) {
                        continue;
                    }
                    Broadcast candidateBroadcast = getOnlyBroadcast(scheduleItem);
                    if (isSameTxlogEntry(subject, scheduleItem, subjectBroadcast, candidateBroadcast)) {
                        desc.appendText(
                                "Found candidate %s (%s) with broadcast [%s - %s]",
                                scheduleItem.getCanonicalUri(),
                                scheduleItem.getTitle(),
                                candidateBroadcast.getTransmissionTime(),
                                candidateBroadcast.getTransmissionEndTime()
                        );
                        scores.updateEquivalent(scheduleItem, scoreOnMatch);
                        generatorComponent.addComponentResult(scheduleItem.getId(), scoreOnMatch.toString());
                    } else {
                        desc.appendText(
                                "Discarded %s (%s) with broadcast [%s - %s]",
                                scheduleItem.getCanonicalUri(),
                                scheduleItem.getTitle(),
                                candidateBroadcast.getTransmissionTime(),
                                candidateBroadcast.getTransmissionEndTime()
                        );
                    }
                }
                desc.finishStage();
            }
        }
        desc.finishStage();
    }

    @Nullable
    private Set<String> expandRegionalChannelUris(String channelUri) {
        if (BBC1_TXLOG_CHANNEL_URIS.contains(channelUri)) {
            return Sets.difference(BBC1_TXLOG_CHANNEL_URIS, ImmutableSet.of(channelUri));
        } else if (BBC2_TXLOG_CHANNEL_URIS.contains(channelUri)) {
            return Sets.difference(BBC2_TXLOG_CHANNEL_URIS, ImmutableSet.of(channelUri));
        }
        return null;
    }

    private ListMultimap<Publisher, Item> filterScheduleByPublisher(ScheduleChannel candidateScheduleChannel) {
        return candidateScheduleChannel.items().stream() // N.B. we need to retain the order
                .collect(MoreCollectors.toImmutableListMultiMap(Item::getPublisher, item -> item));
    }

    private Broadcast getOnlyBroadcast(Item item) {
        //Schedule resolver sticks the schedule slot broadcast as the only broadcast on the first version
        return Iterables.getOnlyElement(
                Iterables.getOnlyElement(item.getVersions()).getBroadcasts()
        );
    }

    private boolean isSameTxlogEntry(Item subject, Item candidate, Broadcast subjectBroadcast, Broadcast candidateBroadcast) {
        return subject.getTitle().equals(candidate.getTitle())
                && around(subjectBroadcast, candidateBroadcast, flexibility);
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime().minus(flexibility);
        DateTime end = broadcast.getTransmissionEndTime().plus(flexibility);

        Set<Channel> channels = channelUris.parallelStream()
                .map(channelResolver::fromUri)
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .collect(MoreCollectors.toImmutableSet());
        return scheduleResolver.unmergedSchedule(
                start,
                end,
                channels,
                publishers
        );
    }

    @Override
    public String toString() {
        return "BARB-BBC Regional Txlog Generator";
    }

    public static final class Builder {
        private ScheduleResolver scheduleResolver;
        private Set<Publisher> publishers;
        private ChannelResolver channelResolver;
        private Predicate<? super Broadcast> broadcastFilter;
        private Duration flexibility;
        private Score scoreOnMatch;

        private Builder() {
        }

        public Builder withScheduleResolver(ScheduleResolver resolver) {
            this.scheduleResolver = resolver;
            return this;
        }

        public Builder withPublishers(Set<Publisher> publishers) {
            this.publishers = publishers;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withBroadcastFilter(@Nullable Predicate<? super Broadcast> broadcastFilter) {
            this.broadcastFilter = broadcastFilter;
            return this;
        }

        public Builder withBroadcastFlexibility(Duration flexibility) {
            this.flexibility = flexibility;
            return this;
        }

        public Builder withScoreOnMatch(Score scoreOnMatch) {
            this.scoreOnMatch = scoreOnMatch;
            return this;
        }

        public BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer build() {
            return new BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer(this);
        }
    }
}
