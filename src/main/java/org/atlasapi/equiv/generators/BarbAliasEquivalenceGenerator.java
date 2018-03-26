package org.atlasapi.equiv.generators;


import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Alias;
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
        return new BarbAliasEquivalenceGenerator<>(lookupEntryStore, resolver);
    }

    @Override
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        if (!(subject.getAliases().isEmpty())) {
            equivalents = findByCommonAlias(subject, equivalents, desc, equivToTelescopeResults);
        }

        return equivalents.build();
    }

    private DefaultScoredCandidates.Builder<T> findByCommonAlias(
            T subject,
            DefaultScoredCandidates.Builder<T> equivalents,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {

        desc.startStage("Resolving Barb Aliases:");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Barb Alias Equivalence Generator");

        subject.getAliases().forEach(alias -> desc.appendText(
                "namespace: " +
                        alias.getNamespace() +
                        ", value: " +
                        alias.getValue()
        ));
        desc.finishStage();

        Set<Iterable<LookupEntry>> entriesSet = subject.getAliases().stream().map(alias ->
            lookupEntryStore.entriesForAliases(
                    Optional.of(alias.getNamespace()),
                    ImmutableSet.of(alias.getValue())
            )).collect(Collectors.toSet());

        entriesSet.stream().filter(Objects::nonNull).forEach(iterableLookupEntry ->
            iterableLookupEntry.forEach(entry -> {
                Identified identified = resolver.findByCanonicalUris(
                        ImmutableSet.of(entry.uri())
                ).getFirstValue().requireValue();

                if (!identified.getAliases().isEmpty()) {
                    boolean match = false;

                    for (Alias alias : identified.getAliases()) {
                        if (subject.getAliases().contains(alias)) {
                            match = true;
                            break;
                        }
                    }

                    if (match) {
                        equivalents.addEquivalent((T) identified, Score.ONE);
                        desc.appendText("Resolved %s", identified.getCanonicalUri());

                        // this if statement keeps lots of old tests happy
                        if (identified.getId() != null) {
                            generatorComponent.addComponentResult(identified.getId(), "1.0");
                        }
                    }
                }
            })
        );

        equivToTelescopeResults.addGeneratorResult(generatorComponent);

        return equivalents;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
