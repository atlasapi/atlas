package org.atlasapi.equiv.generators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;

import static com.google.common.base.Preconditions.checkNotNull;

public class AliasResolvingEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final ContentResolver resolver;
    private final LookupEntryStore lookupEntryStore;
    private final Set<Publisher> publishers;
    private final Set<Set<String>> namespacesSet;
    private final Score aliasMatchingScore;
    private final boolean includeUnpublishedContent;
    private final Class<T> cls;

    private AliasResolvingEquivalenceGenerator(Builder builder) {
        this.resolver = checkNotNull(builder.resolver);
        this.publishers = checkNotNull(builder.publishers);
        this.lookupEntryStore = checkNotNull(builder.lookupEntryStore);
        this.namespacesSet = checkNamespacesSet(builder.namespacesSet);
        this.aliasMatchingScore = checkNotNull(builder.aliasMatchingScore);
        this.includeUnpublishedContent = checkNotNull(builder.includeUnpublishedContent);
        this.cls = checkNotNull(builder.cls);
    }

    private Set<Set<String>> checkNamespacesSet(@Nullable Set<Set<String>> namespacesSet) {

        if (namespacesSet == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Set<String>> builder = ImmutableSet.builder();

        Set<String> masterSet = new HashSet<>();
        for (Set<String> namespaces : namespacesSet) {
            for (String namespace : namespaces) {
                if (!masterSet.contains(namespace)) {
                    masterSet.add(namespace);
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Found a namespace (%s) contained in multiple namespace sets",
                            namespace
                    ));
                }
            }
            builder.add(ImmutableSet.copyOf(namespaces));
        }

        return builder.build();
    }

    private Set<Alias> getExpandedAliases(Set<Alias> aliases) {
        Set<Alias> expandedAliases = new HashSet<>();

        for (Alias alias : aliases) {
            expandedAliases.add(alias);

                for (Set<String> namespaces : namespacesSet) {

                    if (namespaces.contains(alias.getNamespace())) {
                        expandedAliases.addAll(
                                namespaces.stream()
                                        .filter(ns -> !ns.equals(alias.getNamespace()))
                                        .map(ns -> new Alias(ns, alias.getValue()))
                                        .collect(Collectors.toSet())
                        );
                        break; // okay to break here because of checkNamespacesSet validation
                    }
                }
        }
        return expandedAliases;
    }

    @Override
    public ScoredCandidates<T> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        DefaultScoredCandidates.Builder<T> candidates = DefaultScoredCandidates.fromSource("Alias");
        desc.startStage("Resolving aliases:");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Alias Resolving Equivalence Generator");

        for (Alias alias : getExpandedAliases(content.getAliases())) {

            desc.appendText("namespace: %s, value: %s", alias.getNamespace(), alias.getValue());

            Iterable<LookupEntry> lookupEntries = lookupEntryStore.entriesForAliases(
                    Optional.of(alias.getNamespace()),
                    ImmutableSet.of(alias.getValue()),
                    publishers,
                    includeUnpublishedContent
            );

            List<String> lookupUris = MoreStreams.stream(lookupEntries)
                    .map(LookupEntry::uri)
                    .collect(MoreCollectors.toImmutableList());

            ResolvedContent resolved = resolver.findByCanonicalUris(lookupUris);
            for (T identified : Iterables.filter(resolved.getAllResolvedResults(), cls)) {
                if (identified.isActivelyPublished()) {
                    candidates.addEquivalent(identified, aliasMatchingScore);
                    desc.appendText("Resolved %s", identified.getCanonicalUri());
                    generatorComponent.addComponentResult(identified.getId(), aliasMatchingScore.toString());
                }
            }

            if (resolved.getUnresolved().size() > 0) {
                desc.appendText("Missed %s", resolved.getUnresolved().size());
            }
        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);
        desc.finishStage();
        return candidates.build();
    }
    
    @Override
    public String toString() {
        return "Alias Resolving Generator";
    }

    public static <T extends Content> Builder<T> builder() {
        return new Builder<>();
    }

    public static final class Builder<T extends Content> {

        private ContentResolver resolver;
        private LookupEntryStore lookupEntryStore;
        private Set<Publisher> publishers;
        private Set<Set<String>> namespacesSet;
        private Score aliasMatchingScore;
        private Boolean includeUnpublishedContent;
        private Class<T> cls;

        private Builder() {
        }

        public Builder<T> withResolver(ContentResolver resolver) {
            this.resolver = resolver;
            return this;
        }

        public Builder<T> withLookupEntryStore(LookupEntryStore lookupEntryStore) {
            this.lookupEntryStore = lookupEntryStore;
            return this;
        }

        public Builder<T> withPublishers(Set<Publisher> publishers) {
            this.publishers = publishers;
            return this;
        }

        public Builder<T> withNamespacesSet(@Nullable Set<Set<String>> namespacesSet) {
            this.namespacesSet = namespacesSet;
            return this;
        }

        public Builder<T> withAliasMatchingScore(Score aliasMatchingScore) {
            this.aliasMatchingScore = aliasMatchingScore;
            return this;
        }

        public Builder<T> withIncludeUnpublishedContent(Boolean includeUnpublishedContent) {
            this.includeUnpublishedContent = includeUnpublishedContent;
            return this;
        }

        public Builder<T> withClass(Class<T> cls) {
            this.cls = cls;
            return this;
        }

        public AliasResolvingEquivalenceGenerator<T> build() {
            return new AliasResolvingEquivalenceGenerator<>(this);
        }
    }
}
