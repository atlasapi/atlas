package org.atlasapi.equiv.generators;


import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
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

import java.util.Objects;
import java.util.Set;

public class BarbAliasEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private static final String NAME = "Barb Alias Resolving Generator";
    private static final int MAXIMUM_ALIAS_MATCHES = 50;
    private static final double ALIAS_MATCHING_SCORE = 10.0;

    private static final String TXNUMBER_NAMESPACE_SUFFIX = "txnumber";
    private static final String BCID_NAMESPACE_SUFFIX = "bcid";
    private static final String BROADCAST_GROUP_NAMESPACE_PREFIX = "gb:barb:broadcastGroup";
    private static final String ORIGINATING_OWNER_NAMESPACE_PREFIX = "gb:barb:originatingOwner:broadcastGroup";


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
                "namespace: " + alias.getNamespace() + ", value: " + alias.getValue()
        ));
        desc.finishStage();

        Set<LookupEntry> entriesSet = subject.getAliases().parallelStream()
                .filter(alias -> !alias.getNamespace().endsWith(TXNUMBER_NAMESPACE_SUFFIX))
                .map(this::getLookupEntries)
                .filter(Objects::nonNull)
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        Set<LookupEntry> candidateEntries = subject.getAliases().parallelStream()
                .filter(alias -> !alias.getNamespace().endsWith(TXNUMBER_NAMESPACE_SUFFIX)) //don't equiv on txnumber
                .map(alias ->
                        entriesSet.stream()
                                .filter(entry -> !entry.uri().equals(subject.getCanonicalUri())
                                        && matchesAlias(entry, alias))
                                .collect(MoreCollectors.toImmutableSet()))
                .filter(collection -> collection.size() <= MAXIMUM_ALIAS_MATCHES) //avoids equiving on aliases which are too common
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolved = resolver.findByCanonicalUris(candidateEntries.stream()
                .map(LookupEntry::uri)
                .collect(MoreCollectors.toImmutableSet())
        );

        resolved.getAllResolvedResults().stream()
                .distinct()
                .forEach(identified -> {
                    equivalents.addEquivalent((T) identified, Score.valueOf(ALIAS_MATCHING_SCORE));
                    desc.appendText("Candidate %s", identified.getCanonicalUri());

                    // this if statement keeps lots of old tests happy
                    if (identified.getId() != null) {
                        generatorComponent.addComponentResult(identified.getId(), String.valueOf(ALIAS_MATCHING_SCORE));
                    }
                });

        equivToTelescopeResults.addGeneratorResult(generatorComponent);

        return equivalents;
    }

    private Iterable<LookupEntry> getLookupEntries(Alias alias) {
        Iterable<LookupEntry> entriesForAlias = lookupEntryStore.entriesForAliases(
                Optional.of(alias.getNamespace()),
                ImmutableSet.of(alias.getValue()));
        if(alias.getNamespace().startsWith(ORIGINATING_OWNER_NAMESPACE_PREFIX)) { //also generate candidates for the original namespce
            return Iterables.concat(entriesForAlias,
                    lookupEntryStore.entriesForAliases(
                            Optional.of(
                                    replaceNamespace(
                                            alias.getNamespace(),
                                            ORIGINATING_OWNER_NAMESPACE_PREFIX,
                                            BROADCAST_GROUP_NAMESPACE_PREFIX)
                            ),
                            ImmutableSet.of(alias.getValue()))
            );
        } else if(alias.getNamespace().startsWith(BROADCAST_GROUP_NAMESPACE_PREFIX)) { //also generate candidates for base namespce
            return Iterables.concat(entriesForAlias,
                    lookupEntryStore.entriesForAliases(
                            Optional.of(
                                    replaceNamespace(
                                            alias.getNamespace(),
                                            BROADCAST_GROUP_NAMESPACE_PREFIX,
                                            ORIGINATING_OWNER_NAMESPACE_PREFIX)
                            ),
                            ImmutableSet.of(alias.getValue()))
            );
        } else {
            return entriesForAlias;
        }
    }

    private boolean matchesAlias(LookupEntry entry, Alias alias) {
        if (entry.aliases().contains(alias)) {
            return true;
        }
        //equiv between originating owner and broadcast group namespaces
        if (alias.getNamespace().startsWith(BROADCAST_GROUP_NAMESPACE_PREFIX)
                && entry.aliases().contains(
                        aliasForNewNamespace(alias, BROADCAST_GROUP_NAMESPACE_PREFIX, ORIGINATING_OWNER_NAMESPACE_PREFIX))
                )
        {
            return true;
        }
        if (alias.getNamespace().startsWith(ORIGINATING_OWNER_NAMESPACE_PREFIX)
                && entry.aliases().contains(
                        aliasForNewNamespace(alias, ORIGINATING_OWNER_NAMESPACE_PREFIX, BROADCAST_GROUP_NAMESPACE_PREFIX))
                )
        {
            return true;
        }
        return false;
    }

    private Alias aliasForNewNamespace(Alias oldAlias, String oldNamespacePrefix, String newNamespacePrefix) {
        return new Alias(
                replaceNamespace(oldAlias.getNamespace(), oldNamespacePrefix, newNamespacePrefix),
                oldAlias.getValue()
        );
    }

    private String replaceNamespace(String namespace, String oldNamespacePrefix, String newNamespacePrefix) {
        return newNamespacePrefix.concat(namespace.substring(oldNamespacePrefix.length()));
    }

    @Override
    public String toString() {
        return NAME;
    }
}
