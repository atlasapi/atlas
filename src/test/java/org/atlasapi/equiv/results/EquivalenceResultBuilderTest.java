package org.atlasapi.equiv.results;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.ScoreCombiner;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.TopEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class EquivalenceResultBuilderTest {

    @Test
    public void testResultFor() {

        ScoreCombiner<Item> combiner = new AddingEquivalenceCombiner<Item>();
        EquivalenceFilter<Item> filter = AlwaysTrueFilter.get();
        EquivalenceExtractor<Item> extractor = new TopEquivalenceExtractor<Item>();
        EquivalenceResultBuilder<Item> builder = new DefaultEquivalenceResultBuilder<Item>(combiner, filter, ImmutableList
                .of(extractor));

        Item item = new Item("testUri","testCurie",Publisher.PA);
        Item bbcItem = new Item("bbcItem", "bbcItemCurie", Publisher.BBC);
        Item c4Item = new Item("c4Item", "c4ItemCurie", Publisher.C4);
        
        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("A Source")
                    .addEquivalent(bbcItem, Score.valueOf(5.0))
                    .addEquivalent(c4Item, Score.valueOf(5.0))
                .build(),
                DefaultScoredCandidates.<Item>fromSource("B Source")
                    .addEquivalent(bbcItem, Score.valueOf(5.0))
                    .addEquivalent(c4Item, Score.valueOf(5.0))
                .build()
        );

        EquivalenceResult<Item> result = builder.resultFor(
                item,
                equivalents,
                new DefaultDescription(),
                EquivToTelescopeResults.create("id", "publisher")
        );

        ScoredCandidates<Item> equivalences = result.combinedEquivalences();
        
        assertEquals("A Source/B Source", equivalences.source());
        assertEquals(ImmutableMap.of(
                bbcItem, Score.valueOf(10.0),
                c4Item, Score.valueOf(10.0)
        ), equivalences.candidates());
        
        Multimap<Publisher, ScoredCandidate<Item>> strongEquivalences = result.strongEquivalences();
        
        assertEquals(ImmutableMultimap.of(
                Publisher.BBC, ScoredCandidate.valueOf(bbcItem, Score.valueOf(10.0)),
                Publisher.C4, ScoredCandidate.valueOf(c4Item, Score.valueOf(10.0))
        ), strongEquivalences);
        
        
    }

}
