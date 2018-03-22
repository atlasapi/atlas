package org.atlasapi.equiv.generators;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbAliasEquivalenceGeneratorTest {

    // force equivalence
    private BarbAliasEquivalenceGenerator generator;
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final MongoLookupEntryStore lookupEntryStore = mock(MongoLookupEntryStore.class);

    // alias equivalence
    private BarbAliasEquivalenceGenerator aliasGenerator;
    private final ContentResolver aliasResolver = mock(ContentResolver.class);
    private final MongoLookupEntryStore aliasLookupEntryStore = mock(MongoLookupEntryStore.class);

    Content aliasIdentified1;
    Content aliasIdentified2;

    ResultDescription desc;

    @Before
    public void setUp() {

        desc = new DefaultDescription();

        setupForceEquivalenceTests();
        setupAliasEquivalenceTests();
    }

    private void setupAliasEquivalenceTests() {
        Set<Alias> aliasesForaliasIdentified1 = ImmutableSet.of(
                new Alias("namespaceOne", "someBcid"),
                new Alias("namespaceTwo", "someOtherBcid")
        );

        aliasIdentified1 = new Item();
        aliasIdentified1.setCanonicalUri("Uri for alias test");
        aliasIdentified1.setAliases(aliasesForaliasIdentified1);

        aliasIdentified2 = new Item();
        aliasIdentified2.setCanonicalUri("Another uri for alias test");
        aliasIdentified2.setAliases(ImmutableSet.of(
                new Alias("namespaceOne", "someBcid"),
                new Alias("namespaceThree", "someOtherOtherBcid")
        ));

        ResolvedContent aliasResolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("Uri for alias test", aliasIdentified1)
                .build();

        DefaultScoredCandidates.Builder aliasEquivalents =
                DefaultScoredCandidates.fromSource("Barb Alias Matching");

        LookupEntry lookupEntry = new LookupEntry(
                "Uri for alias test",
                22L,
                new LookupRef("Uri for alias test", 23L, Publisher.PA, ContentCategory.CHILD_ITEM),
                ImmutableSet.of("Uri for alias test"),
                aliasesForaliasIdentified1,
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                DateTime.now(),
                DateTime.now(),
                true
        );

        aliasResolvedContent.getAllResolvedResults().forEach(
                aliasIdentified ->
                        aliasEquivalents.addEquivalent(aliasIdentified, Score.ONE));

        when(aliasResolver.findByCanonicalUris(Matchers.anyCollection()))
                .thenReturn(aliasResolvedContent);

        when(aliasLookupEntryStore.entriesForAliases(
                Optional.of("namespaceOne"),
                ImmutableSet.of("someBcid")
        )).thenReturn(ImmutableSet.of(lookupEntry));

        aliasGenerator = new BarbAliasEquivalenceGenerator(aliasLookupEntryStore, aliasResolver);
    }

    private void setupForceEquivalenceTests() {
        List<String> forceTargetUrisOne = new ArrayList<>();
        forceTargetUrisOne.add("http://txlogs.barb.co.uk/episode/00000000000226905561");
        forceTargetUrisOne.add("http://itv.com/episode/1_5576_0002");

        DefaultScoredCandidates.Builder equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        Content identified = new Item();
        identified.setCanonicalUri("http://txlogs.barb.co.uk/episode/00000000000226905561");
        Content identified1 = new Item();
        identified1.setCanonicalUri("http://itv.com/episode/1_5576_0002");

        ResolvedContent resolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("nothing1", identified)
                .put("nothing2", identified1)
                .build();

        resolvedContent.getAllResolvedResults().forEach(i ->
                equivalents.addEquivalent(i, Score.ONE));

        when(resolver.findByCanonicalUris(Matchers.anyCollection())).thenReturn(resolvedContent);

        generator = new BarbAliasEquivalenceGenerator(lookupEntryStore, resolver);
    }

    //@Test TODO: create a new test now that the hardcoded content was removed
    public void generatorFindsHardcodedContent() {
        Content subject = new Item();
        subject.setCanonicalUri("http://cdmf.barb.co.uk/episode/219060");

        ScoredCandidates scoredCandidates = generator.generate(
                subject,
                desc,
                EquivToTelescopeResults.create("id", "publisher")
        );

        Content identified = new Item();
        identified.setCanonicalUri("http://txlogs.barb.co.uk/episode/00000000000226905561");
        Content identified1 = new Item();
        identified1.setCanonicalUri("http://itv.com/episode/1_5576_0002");

        DefaultScoredCandidates.Builder equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        ResolvedContent resolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("nothing1", identified)
                .put("nothing2", identified1)
                .build();

        resolvedContent.getAllResolvedResults().forEach(i -> equivalents.addEquivalent(
                i,
                Score.valueOf(1.0)
        ));

        assertEquals(scoredCandidates, equivalents.build());
    }

    @Test
    public void aliasGeneratorFindsByAlias() {

        ScoredCandidates scoredCandidates = aliasGenerator.generate(
                aliasIdentified2,
                desc,
                EquivToTelescopeResults.create("id", "publisher")
        );

        System.out.println(desc.toString());
        System.out.println(scoredCandidates.toString());

        assertFalse(scoredCandidates.candidates().isEmpty());

        for (Object scoredCandidate : scoredCandidates.candidates().keySet()) {
            assertEquals(scoredCandidate, aliasIdentified1);
        }

    }
}
