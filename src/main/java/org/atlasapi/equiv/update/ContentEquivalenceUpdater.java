package org.atlasapi.equiv.update;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.ContentEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdater<T extends Content> implements EquivalenceUpdater<T> {

    private final Set<EquivalenceResultUpdater<T>> equivalenceResultUpdaters;
    private final EquivalenceResultHandler<T> handler;
    private final EquivalenceResultMessenger<T> messenger;

    private final EquivalenceUpdaterMetadata metadata;

    private ContentEquivalenceUpdater(Builder<T> builder) {
       this.equivalenceResultUpdaters = ImmutableSet.copyOf(builder.equivalenceResultUpdaters);
        this.handler = checkNotNull(builder.handler);
        this.messenger = checkNotNull(builder.messenger);
        this.metadata = ContentEquivalenceUpdaterMetadata.builder()
                .withEquivalenceResultUpdaters(this.equivalenceResultUpdaters)
                .withHandler(handler)
                .build();
    }

    public static <T extends Content> EquivalenceResultUpdatersStep<T> builder() {
        return new Builder<>();
    }

    @Override
    public boolean updateEquivalences(T content, OwlTelescopeReporter telescope) {
        ReadableDescription desc = new DefaultDescription();

        EquivToTelescopeResults resultsForTelescope = EquivToTelescopeResults.create(
                String.valueOf(content.getId()),
                content.getPublisher().toString()
        );

        List<ScoredCandidates<T>> rawScores = new ArrayList<>();
        Map<T, Score> combinedScores = new HashMap<>();
        Multimap<Publisher, ScoredCandidate<T>> strongEquivalences = LinkedListMultimap.create();

        for(EquivalenceResultUpdater<T> equivalenceResultUpdater : equivalenceResultUpdaters) {
            EquivalenceResult<T> result = equivalenceResultUpdater.provideEquivalenceResult(content, telescope);
            rawScores.addAll(result.rawScores());
            combinedScores.putAll(result.combinedEquivalences().candidates());
            strongEquivalences.putAll(result.strongEquivalences());
            desc.appendText(result.description().toString());
        }

        EquivalenceResult<T> result = new EquivalenceResult<>(
                content,
                rawScores,
                DefaultScoredCandidates.fromMappedEquivs("ContentEquivalenceUpdater", combinedScores),
                strongEquivalences,
                desc
        );

        boolean handledWithStateChange = handler.handle(result);

        if (handledWithStateChange) {
            messenger.sendMessage(result);
        }

        telescope.reportSuccessfulEvent(
                content.getId(),
                content.getAliases(),
                content,
                resultsForTelescope
        );

        return !result.combinedEquivalences().candidates().isEmpty();
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return metadata;
    }

    public interface  EquivalenceResultUpdatersStep<T extends Content> {
        HandlerStep<T> withEquivalenceResultUpdaters(Set<EquivalenceResultUpdater<T>> equivalenceResultUpdaters);
    }

    public interface HandlerStep<T extends Content> {

        MessengerStep<T> withHandler(EquivalenceResultHandler<T> handler);
    }

    public interface MessengerStep<T extends Content> {

        BuildStep<T> withMessenger(EquivalenceResultMessenger<T> messenger);
    }

    public interface BuildStep<T extends Content> {

        ContentEquivalenceUpdater<T> build();
    }

    public static class Builder<T extends Content> implements EquivalenceResultUpdatersStep<T>, HandlerStep<T>,
            MessengerStep<T>, BuildStep<T> {

        private Set<EquivalenceResultUpdater<T>> equivalenceResultUpdaters;
        private EquivalenceResultHandler<T> handler;
        private EquivalenceResultMessenger<T> messenger;

        private Builder() {
        }

        @Override
        public HandlerStep<T> withEquivalenceResultUpdaters(
                Set<EquivalenceResultUpdater<T>> equivalenceResultUpdaters
        ) {
            this.equivalenceResultUpdaters = equivalenceResultUpdaters;
            return this;
        }

        @Override
        public MessengerStep<T> withHandler(EquivalenceResultHandler<T> handler) {
            this.handler = handler;
            return this;
        }

        @Override
        public BuildStep<T> withMessenger(EquivalenceResultMessenger<T> messenger) {
            this.messenger = messenger;
            return this;
        }

        public ContentEquivalenceUpdater<T> build() {
            return new ContentEquivalenceUpdater<>(this);
        }
    }
}
