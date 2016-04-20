package org.atlasapi.equiv.generators;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceGeneratorsTest {

    private EquivalenceGenerators<Content> generators;
    private Set<String> excludedUris;
    @Mock
    private EquivalenceGenerator<Content> generator;
    @Mock
    private ResultDescription resultDescription;
    @Mock
    private ScoredCandidates candidates;

    @Before
    public void setUp() {
        excludedUris = ImmutableSet.of("excluded");
        generators = EquivalenceGenerators.from(Collections.singleton(generator), excludedUris);
        when(generator.generate(any(Content.class), any(ResultDescription.class))).thenReturn(candidates);
    }

    @Test
    public void generatorCreatesNoCandidatesForExcludedUri() {
        Item item = new Item();
        item.setCanonicalUri("excluded");
        List<ScoredCandidates<Content>> generated = generators.generate(item, resultDescription);
        assertTrue(generated.isEmpty());
    }

    @Test
    public void generatorCreatesCandidatesForNonExcludedUri() {
        Item item = new Item();
        item.setCanonicalUri("notexcluded");
        generators.generate(item, resultDescription);
        verify(generator).generate(item, resultDescription);
    }

}