package org.atlasapi.equiv.generators;

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.google.gdata.util.common.base.Preconditions.checkArgument;

/**
 * Query for each alias. For each alias get a list of candidates, and score them as well (?).
 *
 * @param <T>
 */
public class BarbAliasEquivalenceGeneratorAndScorer<T extends Content> implements EquivalenceGenerator<T> {

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private static final String NAME = "Barb Alias Resolving Generator";
    //If there are more than MAXIMUM_ALIAS_MATCHES candidates for an alias then we don't accept it
    //This is so as to not match content with bad aliases together (such as alias contentId 1).
    private static final int MAXIMUM_ALIAS_MATCHES = 50;
    //Score on match.
    private final Set<Publisher> publishers;
    private final Score aliasMatchingScore;
    private final Score aliasMismatchScore;
    private final boolean includeUnpublishedContent;

    private static final String TXNUMBER_NAMESPACE_SUFFIX = "txnumber";
    private static final String BCID_NAMESPACE_SUFFIX = "bcid";
    private static final String PARENT_VERSION_BCID_NAMESPACE_SUFFIX = "parentVersionBcid";
    private static final BiMap<String, String> T1_BROADCAST_GROUP_BCID_ALIAS_MAP = ImmutableBiMap.of(
            "gb:barb:broadcastGroup:1:bcid", "gb:bbc:nitro:prod:version:pid",
            "gb:barb:broadcastGroup:2:bcid", "gb:itv:production:id",
            "gb:barb:broadcastGroup:3:bcid", "gb:channel4:prod:pmlsd:programmeId",
            "gb:barb:broadcastGroup:4:bcid", "gb:c5:bcid",
            "gb:barb:broadcastGroup:63:bcid", "gb:uktv:bcid"
    );
    private static final ImmutableSet<String> T1_BCID_NAMESPACES =
            ImmutableSet.copyOf(T1_BROADCAST_GROUP_BCID_ALIAS_MAP.values());

    private static final String BG_PREFIX = "gb:barb:broadcastGroup";
    private static final String OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup";
    private static final String ITV_BG_PREFIX = "gb:barb:broadcastGroup:2";
    private static final String ITV_OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup:2";
    private static final String STV_BG_PREFIX = "gb:barb:broadcastGroup:111";
    private static final String STV_OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup:111";
    private static final String SKY_BG_PREFIX = "gb:barb:broadcastGroup:5";
    private static final String SKY_OOBG_PREFIX = "gb:barb:originatingOwner:broadcastGroup:5";
    private static final String SKY_BCID_NAMESPACE = "gb:barb:broadcastGroup:5:bcid";
    private static final String SKY_PARENT_BCID_NAMESPACE = "gb:barb:broadcastGroup:5:parentVersionBcid";
    private static final String BARB_CONTENT_ID_NAMESPACE = "gb:barb:contentid";
    
    private static final Set<String> C4_NAMESPACES = ImmutableSet.of(
            "gb:barb:broadcastGroup:3:bcid",
            "gb:channel4:prod:pmlsd:programmeId",
            "gb:barb:broadcastGroup:3:parentVersionBcid",
            "gb:barb:originatingOwner:broadcastGroup:3:bcid"
    );
    private static final Set<String> C4_BCID_PREFIXES = ImmutableSet.of("C4:", "E4:", "M4:");
    private static final int C4_PREFIX_LENGTH = 3;
    private static final String C4_BG_NAMESPACE = "gb:barb:broadcastGroup:3:bcid";
    

    public BarbAliasEquivalenceGeneratorAndScorer(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Set<Publisher> publishers,
            Score aliasMatchingScore,
            Score aliasMismatchScore,
            boolean includeUnpublishedContent
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
        this.publishers = publishers;
        this.aliasMatchingScore = aliasMatchingScore;
        this.aliasMismatchScore = aliasMismatchScore;
        this.includeUnpublishedContent = includeUnpublishedContent;
    }

    @Override
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        if (!(subject.getAliases().isEmpty())) {
            equivalents = findByCommonAlias(subject, equivalents, desc, equivToTelescopeResult);
        }

        return equivalents.build();
    }

    //Only equiving on BCID and barb contentId aliases due to txnumber and transmission aliases both leading to bad candidates
    private DefaultScoredCandidates.Builder<T> findByCommonAlias(
            T subject,
            DefaultScoredCandidates.Builder<T> equivalents,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
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
                .filter(this::acceptedAlias)
                .map(this::getLookupEntries) //from atlas
                .filter(Objects::nonNull)
                .flatMap(MoreStreams::stream)
                .filter(entry -> publishers.contains(entry.lookupRef().publisher()))
                .collect(MoreCollectors.toImmutableSet());

        //whoever wrote this had trust issues and is rechecking that what he got back is actually
        //matching content. We assume he had good reason, but might as well be pointless.
        Set<LookupEntry> candidatesSet = expandedAliases.parallelStream()
                .filter(this::acceptedAlias)
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
                .forEach(candidate -> {
                    Score score = score(subject, candidate, expandedAliases);
                    equivalents.addEquivalent((T) candidate, score);
                    desc.appendText("Candidate %s", candidate.getCanonicalUri());

                    // this if statement keeps lots of old tests happy
                    if (candidate.getId() != null) {
                        generatorComponent.addComponentResult(candidate.getId(),
                                aliasMatchingScore.toString());
                    }
                });

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return equivalents;
    }

    private Score score(T subject, Identified candidate, Set<Alias> expandedAliases) {
        boolean subjectIsSkyWithNonSkyOobgid = skyWithNonSkyOobgid(subject);
        boolean candidateIsSkyWithNonSkyOobgid = skyWithNonSkyOobgid(candidate);
        boolean eitherIsSkyWithNonSkyOobgid = subjectIsSkyWithNonSkyOobgid || candidateIsSkyWithNonSkyOobgid;
        for (Alias alias : candidate.getAliases()) {
            // Sky parent version bcids should be ignored if the originating owner of the content is non-Sky
            // According to Sky the parent version bcids in these cases can (and have been observed) to be incorrect
            // and would equiv content that should not be equived together.
            // As a result if either the subject or candidate is such a piece of sky content we have to be careful
            // to avoid equiving through the parent version bcid incorrectly.
            // We can't prevent generating the candidates since we don't know until we resolve whether the candidate
            // has a non-Sky originating owner, and as such do this in the scoring step.
            if (eitherIsSkyWithNonSkyOobgid && alias.getNamespace().startsWith(SKY_BG_PREFIX)) {
                if (alias.getNamespace().equals(SKY_PARENT_BCID_NAMESPACE)) {
                    if(!candidateIsSkyWithNonSkyOobgid) { //the parent bcid should be used, but the subject's parent bcid should be ignored
                        Alias bcidAlias = aliasForNewNamespaceSuffix(
                                alias,
                                PARENT_VERSION_BCID_NAMESPACE_SUFFIX,
                                BCID_NAMESPACE_SUFFIX
                        );
                        if (subject.getAliases().contains(bcidAlias)) {
                            return aliasMatchingScore;
                        }
                    }
                } else if (alias.getNamespace().equals(SKY_BCID_NAMESPACE)) {
                    if (subject.getAliases().contains(alias)) {
                        return aliasMatchingScore;
                    } else if (!subjectIsSkyWithNonSkyOobgid) { //the subject's parent bcid should be considered
                        Alias parentBcidAlias = aliasForNewNamespaceSuffix(
                                alias,
                                BCID_NAMESPACE_SUFFIX,
                                PARENT_VERSION_BCID_NAMESPACE_SUFFIX
                        );
                        if (subject.getAliases().contains(parentBcidAlias)) {
                            return aliasMatchingScore;
                        }
                    }
                }
            } else {
                if (expandedAliases.contains(alias)) {
                    return aliasMatchingScore;
                }
            }
        }
        return aliasMismatchScore;
    }

    private boolean skyWithNonSkyOobgid(Identified subject) {
        boolean isSky = false;
        boolean isNonSkyOobgid = false;
        for(Alias alias : subject.getAliases()) {
            if(alias.getNamespace().startsWith(SKY_BG_PREFIX)) {
                isSky = true;
            } else if(alias.getNamespace().startsWith(OOBG_PREFIX) && !alias.getNamespace().equals(SKY_OOBG_PREFIX)) {
                isNonSkyOobgid = true;
            }
            if(isSky && isNonSkyOobgid) {
                return true;
            }
        }
        return false;
    }

    private boolean acceptedAlias(Alias alias) {
        if(alias.getNamespace().endsWith(BCID_NAMESPACE_SUFFIX)
                || alias.getNamespace().endsWith(PARENT_VERSION_BCID_NAMESPACE_SUFFIX)
                || alias.getNamespace().equalsIgnoreCase(BARB_CONTENT_ID_NAMESPACE)
        ) {
            return true;
        }
        for(String namespace : T1_BCID_NAMESPACES) {
            if(alias.getNamespace().equalsIgnoreCase(namespace)) {
                return true;
            }
        }
        return false;
    }

    private ImmutableSet<LookupEntry> getAllThatMatch(Alias alias, Set<LookupEntry> entriesSet, T subject) {
        return entriesSet.stream()
                .filter(entry -> !isItself(subject, entry) && entry.aliases().contains(alias))
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
            if (alias.getNamespace().startsWith(OOBG_PREFIX)) { //add bcid and parentVersionBcid aliases
                Alias bgAliasForOobg = aliasForNewNamespacePrefix(alias, OOBG_PREFIX, BG_PREFIX);
                expandedAliases.add(bgAliasForOobg);
                //at the time of writing this, this should always be true
                if(bgAliasForOobg.getNamespace().endsWith(BCID_NAMESPACE_SUFFIX)) {
                    expandedAliases.add(
                            aliasForNewNamespaceSuffix(bgAliasForOobg, BCID_NAMESPACE_SUFFIX, PARENT_VERSION_BCID_NAMESPACE_SUFFIX)
                    );
                }
            } else if (alias.getNamespace().startsWith(BG_PREFIX)) {
                if(alias.getNamespace().endsWith(BCID_NAMESPACE_SUFFIX)) { //add oobcid and parentVersionBcid aliases
                    expandedAliases.add(aliasForNewNamespacePrefix(alias, BG_PREFIX, OOBG_PREFIX));
                    expandedAliases.add(
                            aliasForNewNamespaceSuffix(alias, BCID_NAMESPACE_SUFFIX, PARENT_VERSION_BCID_NAMESPACE_SUFFIX)
                    );
                } else if(alias.getNamespace().endsWith(PARENT_VERSION_BCID_NAMESPACE_SUFFIX)) { //add bcid and oobbcid aliases
                    Alias bcidAliasFromParentBcid =
                            aliasForNewNamespaceSuffix(alias, PARENT_VERSION_BCID_NAMESPACE_SUFFIX, BCID_NAMESPACE_SUFFIX);
                    expandedAliases.add(bcidAliasFromParentBcid);
                    expandedAliases.add(aliasForNewNamespacePrefix(bcidAliasFromParentBcid, BG_PREFIX, OOBG_PREFIX));
                }
            } else if (T1_BROADCAST_GROUP_BCID_ALIAS_MAP.containsValue(alias.getNamespace())) {
                String bgNamespace = T1_BROADCAST_GROUP_BCID_ALIAS_MAP.inverse().get(alias.getNamespace());

                String aliasValue = bgNamespace.equals(C4_BG_NAMESPACE) && isC4BcidPrefix(alias.getValue())
                        ? alias.getValue().substring(C4_PREFIX_LENGTH) //C4 CMS bcids do not include this prefix
                        : alias.getValue();
                
                C4_BCID_PREFIXES.forEach(prefix -> {
                    Alias bgAliasForCms = new Alias(
                            bgNamespace,
                            bgNamespace.equals(C4_BG_NAMESPACE)
                                    ? prefix.concat(aliasValue)
                                    : aliasValue
                    );
                    expandedAliases.add(bgAliasForCms);
                    checkArgument(bgAliasForCms.getNamespace().startsWith(BG_PREFIX));
                    expandedAliases.add(
                            aliasForNewNamespacePrefix(bgAliasForCms, BG_PREFIX, OOBG_PREFIX)
                    );
                    expandedAliases.add(
                            aliasForNewNamespaceSuffix(bgAliasForCms, BCID_NAMESPACE_SUFFIX, PARENT_VERSION_BCID_NAMESPACE_SUFFIX)
                    );
                });
            }
        }

        for (Alias alias : ImmutableSet.copyOf(expandedAliases)) {
            // if you have the ITV add the STV, and vs.
            // The previous block would have already expanded that for Originating Owner.
            if (alias.getNamespace().startsWith(ITV_BG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespacePrefix(alias, ITV_BG_PREFIX, STV_BG_PREFIX));
            } else if (alias.getNamespace().startsWith(ITV_OOBG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespacePrefix(alias, ITV_OOBG_PREFIX, STV_OOBG_PREFIX));
            } else if (alias.getNamespace().startsWith(STV_BG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespacePrefix(alias, STV_BG_PREFIX, ITV_BG_PREFIX));
            } else if (alias.getNamespace().startsWith(STV_OOBG_PREFIX)) {
                expandedAliases.add(aliasForNewNamespacePrefix(alias, STV_OOBG_PREFIX, ITV_OOBG_PREFIX));
            }
        }

        for (Alias alias : ImmutableSet.copyOf(expandedAliases)) {
            String value = C4_NAMESPACES.contains(alias.getNamespace()) && isC4BcidPrefix(alias.getValue())
                            ? alias.getValue().substring(C4_PREFIX_LENGTH) //C4 CMS bcids do not include this prefix
                            : alias.getValue();
            
            if (T1_BROADCAST_GROUP_BCID_ALIAS_MAP.containsKey(alias.getNamespace())) {
                String cmsNamespace = T1_BROADCAST_GROUP_BCID_ALIAS_MAP.get(alias.getNamespace());
                
                expandedAliases.add(new Alias(cmsNamespace, value));
                addPrefixedC4Aliases(expandedAliases, cmsNamespace, value);
            }
            
            addPrefixedC4Aliases(expandedAliases, alias.getNamespace(), value);
        }
        
        return expandedAliases;
    }
    
    private void addPrefixedC4Aliases(Set<Alias> expandedAliases, String namespace, String value) {
        if (C4_NAMESPACES.contains(namespace)) {
            C4_BCID_PREFIXES.forEach(prefix ->
                    expandedAliases.add(new Alias(namespace, prefix.concat(value)))
            );
            expandedAliases.add(new Alias(namespace, value)); // Add a non-prefixed version to the namespace too
        }
    }
    
    private boolean isC4BcidPrefix(String value) {
        return C4_BCID_PREFIXES.stream().anyMatch(value::startsWith);
    }

    private Iterable<LookupEntry> getLookupEntries(Alias alias) {
        return lookupEntryStore.entriesForAliases(
                Optional.of(alias.getNamespace()),
                ImmutableSet.of(alias.getValue()),
                includeUnpublishedContent
        );
    }

    private Alias aliasForNewNamespacePrefix(Alias alias, String oldPrefix, String newPrefix) {
        return new Alias(
                replaceNamespacePrefix(alias.getNamespace(), oldPrefix, newPrefix),
                alias.getValue());
    }
    //replaces the oldPrefix with the newPrefix in the given namespace.
    private String replaceNamespacePrefix(String namespace, String oldPrefix, String newPrefix) {
        return newPrefix.concat(namespace.substring(oldPrefix.length()));
    }

    private Alias aliasForNewNamespaceSuffix(Alias alias, String oldSuffix, String newSuffix) {
        return new Alias(
                replaceNamespaceSuffix(alias.getNamespace(), oldSuffix, newSuffix),
                alias.getValue()
        );
    }

    //replaces the oldSuffix with the newSuffix in the given namespace.
    private String replaceNamespaceSuffix(String namespace, String oldSuffix, String newSuffix) {
        return namespace.substring(0, namespace.length() - oldSuffix.length()).concat(newSuffix);
    }

    @Override
    public String toString() {
        return NAME;
    }
}
