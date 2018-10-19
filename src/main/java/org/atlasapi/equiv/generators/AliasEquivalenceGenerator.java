package org.atlasapi.equiv.generators;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class AliasEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private final Class<T> cls;
    private String namespaceToMatch = null;

    public AliasEquivalenceGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Class<T> cls
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
        this.cls = cls;
    }

    public AliasEquivalenceGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Class<T> cls,
            @Nullable String namespaceToMatch
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
        this.cls = cls;
        this.namespaceToMatch = namespaceToMatch;
    }

    @Override
    public ScoredCandidates<T> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        DefaultScoredCandidates.Builder<T> candidates = DefaultScoredCandidates.fromSource("Alias");
        desc.startStage("Resolving aliases:");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Alias Equivalence Generator");

        //for logging purposes
        for (Alias alias : content.getAliases()) {
            desc.appendText(alias.toString());
        }
        desc.finishStage();

        Set<T> resolvedContentForAliases = content.getAliases().parallelStream()
                .filter(alias -> namespaceToMatch == null || alias.getNamespace().equals(namespaceToMatch))
                .map(this::getLookupEntries)
                .flatMap(MoreStreams::stream)
                .map(this::getResolvedContent)
                .map(ResolvedContent::getAllResolvedResults)
                .flatMap(MoreStreams::stream)
                .filter(cls::isInstance)
                .map(cls::cast)
                .collect(MoreCollectors.toImmutableSet());

        for (T identified : resolvedContentForAliases) {
            if (identified.isActivelyPublished()) {
                candidates.addEquivalent(identified, Score.nullScore());
                desc.appendText("Resolved %s", identified.getCanonicalUri());
            }
        }

        equivToTelescopeResults.addGeneratorResult(generatorComponent);

        return candidates.build();
    }

    private ResolvedContent getResolvedContent(LookupEntry lookupEntry) {
        return resolver.findByCanonicalUris(
                ImmutableSet.of(lookupEntry.uri()));
    }

    private Iterable<LookupEntry> getLookupEntries(Alias alias) {
        return lookupEntryStore.entriesForAliases(
                Optional.of(alias.getNamespace()),
                ImmutableSet.of(alias.getValue())
        );
    }

    @Override
    public String toString() {
        return "Alias Resolving Generator";
    }
}
