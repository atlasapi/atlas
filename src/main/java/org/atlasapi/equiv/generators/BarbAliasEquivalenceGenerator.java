package org.atlasapi.equiv.generators;


import java.util.ArrayList;
import java.util.List;
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
import org.atlasapi.persistence.content.ResolvedContent;
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

    @SuppressWarnings("unchecked")
    @Override
    public ScoredCandidates<T> generate(T subject, ResultDescription desc) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        List<String> forceTargetUrisOne = new ArrayList<>();
        forceTargetUrisOne.add("http://txlogs.barb.co.uk/episode/00000000000226905561");
        forceTargetUrisOne.add("http://itv.com/episode/1_5576_0002");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/754382",
                forceTargetUrisOne,
                desc
        );

        List<String> forceTargetUrisTwo = new ArrayList<>();
        forceTargetUrisTwo.add("http://txlogs.barb.co.uk/episode/00000000000228464843");
        forceTargetUrisTwo.add("http://itv.com/episode/9D_13136");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/319451",
                forceTargetUrisTwo,
                desc
        );

        List<String> forceTargetUrisThree = new ArrayList<>();
        forceTargetUrisThree.add("http://txlogs.barb.co.uk/episode/107089899");
        forceTargetUrisThree.add("http://nitro.bbc.co.uk/programmes/b0676dvn");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/150272",
                forceTargetUrisThree,
                desc
        );

        List<String> forceTargetUrisFour = new ArrayList<>();
        forceTargetUrisFour.add("http://txlogs.barb.co.uk/episode/107089910");
        forceTargetUrisFour.add("http://nitro.bbc.co.uk/programmes/b08vd13d");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/825344",
                forceTargetUrisFour,
                desc
        );

        List<String> forceTargetUrisFive = new ArrayList<>();
        forceTargetUrisFive.add("http://uktv.co.uk/ENIE812H");
        forceTargetUrisFive.add("http://txlogs.barb.co.uk/episode/ENIE812H82");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/754384",
                forceTargetUrisFive,
                desc
        );

        List<String> forceTargetUrisSix = new ArrayList<>();
        forceTargetUrisSix.add("http://txlogs.barb.co.uk/episode/ENIE813B82");
        forceTargetUrisSix.add("http://uktv.co.uk/ENIE813B");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/754382",
                forceTargetUrisSix,
                desc
        );

        return equivalents.build();
    }

    private DefaultScoredCandidates.Builder findForcedEquivalents(
            T subject,
            DefaultScoredCandidates.Builder equivalents,
            String forceSubjectUri,
            List<String> forceTargetUris,
            ResultDescription desc
    ) {
        if (subject.getCanonicalUri().equals(forceSubjectUri)) {

            ResolvedContent resolvedContent = resolver.findByCanonicalUris(forceTargetUris);

            resolvedContent.getAllResolvedResults().forEach(identified -> {
                equivalents.addEquivalent((T) identified, Score.ONE);
                desc.appendText("Resolved %s", identified.getCanonicalUri());
            });

        }
        return equivalents;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
