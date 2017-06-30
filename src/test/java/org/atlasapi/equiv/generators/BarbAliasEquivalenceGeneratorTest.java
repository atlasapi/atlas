package org.atlasapi.equiv.generators;

import java.util.ArrayList;
import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import org.junit.Test;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbAliasEquivalenceGeneratorTest {

    private BarbAliasEquivalenceGenerator generator;

    private final ContentResolver resolver = mock(ContentResolver.class);
    private final MongoLookupEntryStore lookupEntryStore = mock(MongoLookupEntryStore.class);

    public BarbAliasEquivalenceGeneratorTest() {



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

    @Test
    public void generatorFindsHardcodedContent() {
        Content subject = new Item();
        subject.setCanonicalUri("http://cdmf.barb.co.uk/episode/219060");

        ResultDescription desc = new ResultDescription() {

            @Override
            public ResultDescription appendText(String format, Object... args) {
                return null;
            }

            @Override
            public ResultDescription startStage(String stageName) {
                return null;
            }

            @Override
            public ResultDescription finishStage() {
                return null;
            }
        };

        ScoredCandidates scoredCandidates = generator.generate(subject, desc);

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
}