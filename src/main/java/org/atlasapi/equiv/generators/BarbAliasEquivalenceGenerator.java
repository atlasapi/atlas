package org.atlasapi.equiv.generators;


import java.util.stream.StreamSupport;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

public class BarbAliasEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private static final String NAME = "Barb Alias Resolving Generator";

    public BarbAliasEquivalenceGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
    }

    public static <T extends Content> EquivalenceGenerator<T> barbAliasResolvingGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver
    ) {
        return new BarbAliasEquivalenceGenerator<T>(lookupEntryStore, resolver);
    }

    @Override
    public ScoredCandidates<T> generate(T subject, ResultDescription desc) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        desc.startStage("Resolving Barb Aliases:");
        subject.getAliasUrls().forEach(alias -> desc.appendText(alias));
        desc.finishStage();

        Iterable<LookupEntry> entries = lookupEntryStore.entriesForAliases(
                Optional.of("aliases"),
                subject.getAliasUrls()
        );

        StreamSupport.stream(entries.spliterator(), false)
                .forEach(
                        entry -> {
                            Identified identified = resolver.findByCanonicalUris(ImmutableSet.of(entry.uri())).getFirstValue().requireValue();
                            if (identified.getAliasUrls().size() != 0 &&
                                    subject.getAliasUrls().contains(identified.getAliasUrls())) {
                                equivalents.addEquivalent((T) identified, Score.ONE);
                                desc.appendText("Resolved %s", identified.getCanonicalUri());
                            }
                        }
                );


        return equivalents.build();
    }

    @Override
    public String toString() {
        return NAME;
    }
}
