package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.BBC1_TXLOG_CHANNEL_URIS;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.BBC2_TXLOG_CHANNEL_URIS;

/**
 * Equiv BBC Txlog entries accross regions if start time, end time and title are all exactly the same.
 * This is further additional work for BBC network linking.
 */
public class BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private static final Set<Publisher> ALL_PUBLISHERS = ImmutableSet.of(
            Publisher.LAYER3_TXLOGS,
            Publisher.BARB_TRANSMISSIONS
    );

    private final ScheduleResolver scheduleResolver;
    private final Set<Publisher> publishers;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Score scoreOnMatch;

    private BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer(Builder builder) {
        this.scheduleResolver = checkNotNull(builder.scheduleResolver);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.publishers = ImmutableSet.copyOf(builder.publishers);
        checkArgument(ALL_PUBLISHERS.containsAll(this.publishers));
        this.broadcastFilter = builder.broadcastFilter == null ? broadcast -> true : builder.broadcastFilter;
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
        checkArgument(ALL_PUBLISHERS.contains(subject.getPublisher()));

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
            EquivToTelescopeComponent generatorComponent
    ) {
        Set<String> channelUris = expandRegionalChannelUris(subjectBroadcast.getBroadcastOn());
        if (channelUris == null) {
            return;
        }

        Schedule schedule = scheduleAround(subjectBroadcast, channelUris, publishers);

        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            for (Item scheduleItem : channel.items()) {
                if (scheduleItem.isActivelyPublished()
                        && !subject.getCanonicalUri().equals(scheduleItem.getCanonicalUri())
                        && isSameTxlogEntry(subject, scheduleItem, subjectBroadcast)
                ) {
                    scores.updateEquivalent(scheduleItem, scoreOnMatch);
                    generatorComponent.addComponentResult(scheduleItem.getId(), scoreOnMatch.toString());
                }
            }
        }
    }

    @Nullable
    private Set<String> expandRegionalChannelUris(String channelUri) {
        Set<String> expandedUris = null;
        if (BBC1_TXLOG_CHANNEL_URIS.contains(channelUri)) {
            expandedUris = BBC1_TXLOG_CHANNEL_URIS;
        }
        if (BBC2_TXLOG_CHANNEL_URIS.contains(channelUri)) {
            expandedUris = BBC2_TXLOG_CHANNEL_URIS;
        }
        return expandedUris == null ? null : Sets.difference(expandedUris, ImmutableSet.of(channelUri));
    }

    private boolean isSameTxlogEntry(Item subject, Item candidate, Broadcast subjectBroadcast) {
        //Schedule resolver sticks the schedule slot broadcast as the only broadcast on the first version
        Broadcast candidateBroadcast = Iterables.getOnlyElement(
                Iterables.getOnlyElement(candidate.getVersions()).getBroadcasts()
        );
        return subject.getTitle().equals(candidate.getTitle())
                && subjectBroadcast.getTransmissionTime().equals(candidateBroadcast.getTransmissionTime())
                && subjectBroadcast.getTransmissionEndTime().equals(candidateBroadcast.getTransmissionEndTime());
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime();
        DateTime end = broadcast.getTransmissionEndTime();

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

        public Builder withScoreOnMatch(Score scoreOnMatch) {
            this.scoreOnMatch = scoreOnMatch;
            return this;
        }

        public BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer build() {
            return new BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer(this);
        }
    }
}
