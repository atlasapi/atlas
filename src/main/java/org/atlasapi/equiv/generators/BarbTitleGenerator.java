package org.atlasapi.equiv.generators;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import joptsimple.internal.Strings;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

/** Generate all txlogs for a specific broadcaster whose titles match the subject's **/
public class BarbTitleGenerator<T extends Content> implements EquivalenceGenerator<T> {
    public static final String BBC_BROADCAST_GROUP = "1";

    private final MongoLookupEntryStore lookupEntryStore;
    private final ContentResolver resolver;
    private static final String NAME = "Barb Title Resolving Generator";
    private static final Pattern BROADCAST_GROUP_PATTERN = Pattern.compile("^gb:barb:broadcastGroup:([0-9]+):.*");
    private static final Joiner JOINER = Joiner.on(',');

    private final Set<String> broadcastGroupsToGenerate;
    private final Score scoreOnTitleMatch;

    private BarbTitleGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Set<String> broadcastGroupsToGenerate,
            Score scoreOnTitleMatch
    ) {
        this.lookupEntryStore = lookupEntryStore;
        this.resolver = resolver;
        this.broadcastGroupsToGenerate = ImmutableSet.copyOf(broadcastGroupsToGenerate);
        this.scoreOnTitleMatch = checkNotNull(scoreOnTitleMatch);
    }

    public static <T extends Content> EquivalenceGenerator<T> barbTitleGenerator(
            MongoLookupEntryStore lookupEntryStore,
            ContentResolver resolver,
            Set<String> broadcastGroupsToGenerate,
            Score scoreOnTitleMatch
    ) {
        return new BarbTitleGenerator<>(
                lookupEntryStore,
                resolver,
                broadcastGroupsToGenerate,
                scoreOnTitleMatch
        );
    }

    @Override
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Title Generator");

        if (!(subject.getAliases().isEmpty())) {
            Set<String> broadcastGroupsToGenerate = subject.getAliases().parallelStream()
                    .map(alias -> BROADCAST_GROUP_PATTERN.matcher(alias.getNamespace()))
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(1))
                    .filter(this.broadcastGroupsToGenerate::contains)
                    .collect(MoreCollectors.toImmutableSet());
            if(!broadcastGroupsToGenerate.isEmpty()) {
                equivalents = findTitlesFromBroadcasters(
                        subject,
                        equivalents,
                        desc,
                        equivToTelescopeResults,
                        broadcastGroupsToGenerate
                );
            }
        }

        return equivalents.build();
    }

    private DefaultScoredCandidates.Builder<T> findTitlesFromBroadcasters(
            T subject,
            DefaultScoredCandidates.Builder<T> equivalents,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults,
            Set<String> broadcastGroups
    ) {

        desc.startStage("Resolving Broadcast Group Aliases:");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Barb Title Generator");

        desc.appendText("Broadcast Groups: " + JOINER.join(broadcastGroups));
        desc.finishStage();

        Iterable<LookupEntry> potentialCandidatesSet = getLookupEntriesForPublisher(Publisher.BARB_TRANSMISSIONS);

        ResolvedContent resolved = resolver.findByCanonicalUris(MoreStreams.stream(potentialCandidatesSet)
                .filter(lookupEntry -> !isItself(subject, lookupEntry))
                .filter(lookupEntry -> isCorrectBroadcastGroup(lookupEntry, broadcastGroups))
                .map(LookupEntry::uri)
                .collect(MoreCollectors.toImmutableSet())
        );

        resolved.getAllResolvedResults().parallelStream()
                .distinct()
                .forEach(identified -> {
                    if(titleMatches(subject, identified)) {
                        equivalents.addEquivalent((T) identified, scoreOnTitleMatch);
                        desc.appendText("Candidate %s", identified.getCanonicalUri());

                        if (identified.getId() != null) {
                            generatorComponent.addComponentResult(identified.getId(), String.valueOf(scoreOnTitleMatch.asDouble()));
                        }
                    }
                });

        equivToTelescopeResults.addGeneratorResult(generatorComponent);

        return equivalents;
    }

    private boolean isCorrectBroadcastGroup(LookupEntry lookupEntry, Set<String> broadcastGroups) {
        return lookupEntry.aliases().parallelStream()
                .map(alias -> BROADCAST_GROUP_PATTERN.matcher(alias.getNamespace()))
                .filter(Matcher::matches)
                .anyMatch(matcher -> broadcastGroups.contains(matcher.group(1)));
    }

    private boolean titleMatches(T subject, Identified identified) {
        if(!(identified instanceof Described)) {
            return false;
        }
        Described described = (Described) identified;
        if(Strings.isNullOrEmpty(subject.getTitle()) || Strings.isNullOrEmpty(described.getTitle())) {
            return false;
        }
        return subject.getTitle().toLowerCase().equals(
                described.getTitle().toLowerCase()
        );
    }

    private boolean isItself(T subject, LookupEntry entry) {
        return entry.uri().equals(subject.getCanonicalUri());
    }


    private Iterable<LookupEntry> getLookupEntriesForPublisher(Publisher publisher) {
        return lookupEntryStore.allEntriesForPublishers(ImmutableSet.of(publisher), ContentListingProgress.START);
    }

    @Override
    public String toString() {
        return NAME;
    }
}
