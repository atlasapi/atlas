package org.atlasapi.equiv.generators;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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

public class BarbAliasEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private static final String NAME = "Barb Alias Resolving Generator";
    private static final int MAXIMUM_ALIAS_MATCHES = 50;
    private static final double ALIAS_MATCHING_SCORE = 10.0;

    private static final String TXNUMBER_NAMESPACE_SUFFIX = "txnumber";
    private static final String BCID_NAMESPACE_SUFFIX = "bcid";
    private static final String BG_PREFIX = "gb:barb:broadcastGroup";
    private static final String OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup";
    private static final String ITV_BG_PREFIX = "gb:barb:broadcastGroup:2:bcid";
    private static final String ITV_OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup:2:bcid";
    private static final String STV_BG_PREFIX = "gb:barb:broadcastGroup:111:bcid";
    private static final String STV_OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup:111:bcid";

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

        Set<Alias> expandedAliases = getExpandedAliases(subject.getAliases());

        Set<LookupEntry> potentialCandidatesSet = expandedAliases.parallelStream()
                .filter(alias -> !alias.getNamespace().endsWith(TXNUMBER_NAMESPACE_SUFFIX))
                .map(this::getLookupEntries) //from atlas
                .filter(Objects::nonNull)
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        //whoever wrote this had trust issues and is rechecking that what he got back is actually
        //matching content. We assume he had good reason, but might as well be pointless.
        Set<LookupEntry> candidatesSet = expandedAliases.parallelStream()
                .filter(alias -> !alias.getNamespace().endsWith(TXNUMBER_NAMESPACE_SUFFIX)) //don't equiv on txnumber
                //for each alias of the subject, look in the potentialCandidatesSet and get those that match
                .map(alias -> getAllThatMatch(alias, potentialCandidatesSet, subject))
                .filter(collection -> collection.size() <= MAXIMUM_ALIAS_MATCHES) //avoids equiving on aliases which are too common
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolved = resolver.findByCanonicalUris(candidatesSet.stream()
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

    private ImmutableSet<LookupEntry> getAllThatMatch(Alias alias, Set<LookupEntry> entriesSet, T subject) {
        return entriesSet.stream()
                .filter(entry -> !isItself(subject, entry) && entryContainsAlias(entry, alias))
                .collect(MoreCollectors.toImmutableSet());
    }

    private boolean isItself(T subject, LookupEntry entry) {
        return entry.uri().equals(subject.getCanonicalUri());
    }

    /**
     * Expand the list of aliases to include things that don't have the same namespace, but should
     * still match. (such as originatingOwnerBcid and Bcid, or ITV and STV.
     * We can then use the expanded list to fetch candidates that have either alias.
     */
    private Set<Alias> getExpandedAliases(Set<Alias> aliases) {
        Set<Alias> expandedAliases = new HashSet<>();
        for (Alias alias : aliases) {
            //add all the existing aliases
            expandedAliases.add(alias);

            //add the originating onwer if you have the normal one, and vs
            if (alias.getNamespace().startsWith(OOBG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, OOBG_PREFIX, BG_PREFIX));
            } else if (alias.getNamespace().startsWith(BG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, BG_PREFIX, OOBG_PREFIX));
            }
        }

        for (Alias alias : ImmutableSet.copyOf(expandedAliases)) {
            // if you have the ITV add the STV, and vs.
            // The previous block would have already expanded that for Originating Owner.
            if (alias.getNamespace().equals(ITV_BG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, ITV_BG_PREFIX, STV_BG_PREFIX));
            } else if (alias.getNamespace().startsWith(ITV_OOBG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, ITV_OOBG_PREFIX, STV_OOBG_PREFIX));
            } else if (alias.getNamespace().startsWith(STV_BG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, STV_BG_PREFIX, ITV_BG_PREFIX));
            } else if (alias.getNamespace().startsWith(STV_OOBG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespace(alias, STV_OOBG_PREFIX, ITV_OOBG_PREFIX));
            }
        }

        return expandedAliases;
    }

    private Iterable<LookupEntry> getLookupEntries(Alias alias) {
        return lookupEntryStore.entriesForAliases(
                Optional.of(alias.getNamespace()),
                ImmutableSet.of(alias.getValue()));
    }

    private boolean entryContainsAlias(LookupEntry entry, Alias alias) {
        if (entry.aliases().contains(alias)) {
            return true;
        }

        return false;
    }

    private Alias aliasForNewNamespace(Alias alias, String oldPrefix, String newPrefix) {
        return new Alias(
                getNewNamespace(alias.getNamespace(), oldPrefix, newPrefix),
                alias.getValue());
    }
    //replaces the oldPrefix with the newPrefix in the given namespace.
    private String getNewNamespace(String namespace, String oldPrefix, String newPrefix) {
        return newPrefix.concat(namespace.substring(oldPrefix.length()));
    }

    @Override
    public String toString() {
        return NAME;
    }
}
