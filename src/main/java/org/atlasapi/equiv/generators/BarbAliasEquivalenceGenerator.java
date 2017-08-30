package org.atlasapi.equiv.generators;


import java.util.ArrayList;
import java.util.List;
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
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        DefaultScoredCandidates.Builder<T> equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        List<String> forceTargetUrisOne = new ArrayList<>();
        forceTargetUrisOne.add("http://txlogs.barb.co.uk/episode/00000000000226905561");
        forceTargetUrisOne.add("http://itv.com/episode/1_5576_0002");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/219060",
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
        forceTargetUrisSix.add("http://nitro.bbc.co.uk/programmes/b08w66kz");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/754382",
                forceTargetUrisSix,
                desc
        );

        List<String> forceTargetUrisSeven = new ArrayList<>();
        forceTargetUrisSeven.add("http://nitro.bbc.co.uk/programmes/b04gsjx3");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/739584",
                forceTargetUrisSeven,
                desc
        );

        List<String> forceTargetUrisEight = new ArrayList<>();
        forceTargetUrisEight.add("http://uktv.co.uk/CTBF519P");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/76891",
                forceTargetUrisEight,
                desc
        );

        List<String> forceTargetUrisNine = new ArrayList<>();
        forceTargetUrisNine.add("http://uktv.co.uk/CTAP124B");

        equivalents = findForcedEquivalents(
                subject,
                equivalents,
                "http://cdmf.barb.co.uk/episode/110710",
                forceTargetUrisNine,
                desc
        );

        if (!(subject.getAliases().isEmpty())) {
            equivalents = findByCommonAlias(subject, equivalents, desc, equivToTelescopeResults);
        }

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

            if (resolvedContent.isEmpty()) {
                return equivalents;
            }
            resolvedContent.getAllResolvedResults().forEach(identified -> {
                equivalents.addEquivalent((T) identified, Score.ONE);
                desc.appendText("Resolved %s", identified.getCanonicalUri());
            });

        }
        return equivalents;
    }

    private DefaultScoredCandidates.Builder findByCommonAlias(
            T subject,
            DefaultScoredCandidates.Builder equivalents,
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

                        generatorComponent.addComponentResult(identified.getId(), "1.0");
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
