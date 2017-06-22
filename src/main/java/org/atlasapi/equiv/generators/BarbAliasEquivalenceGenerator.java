package org.atlasapi.equiv.generators;


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

    private final MongoLookupEntryStore lookupEntryStore,
    private final ContentResolver resolver,
    private final Class<T> cls;
    private static final String NAME = "Barb Alias Resolving Generator";

    public BarbAliasEquivalenceGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Class<T> cls
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
        this.cls = cls;
    }

    public static <T extends Content> EquivalenceGenerator<T> barbAliasResolvingGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Class<T> cls
    ) {
        return new BarbAliasEquivalenceGenerator<T>(lookupEntryStore, resolver, cls);
    }

    @Override
    public ScoredCandidates<T> generate(T subject, ResultDescription desc) {
        // TODO: find out what fromSource means and what source needs to be set
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("");

        desc.startStage("Resolving Barb Aliases:");
        for (String alias : subject.getAliasUrls()) {
            desc.appendText(alias);
        }
        desc.finishStage();

        // TODO find out what namespace is in entries for Aliases
        // I think it's the alias namespace - depends which alias is used
        Iterable<LookupEntry> entries = lookupEntryStore.entriesForAliases(
                Optional.of("aliases"),
                subject.getAliasUrls()
        );


        for (LookupEntry entry : entries) {
            Identified identified = resolver.findByCanonicalUris(ImmutableSet.of(entry.uri())).getFirstValue().requireValue();
            if (identified.getAliasUrls().size() != 0 &&
                    subject.getAliasUrls().contains(identified.getAliasUrls())) {
                equivalents.addEquivalent((T) identified, Score.ONE);
                desc.appendText("Resolved %s", identified.getCanonicalUri());
            }
        }

        return equivalents.build();
    }

    @Override
    public String toString() {
        return NAME;
    }
}
