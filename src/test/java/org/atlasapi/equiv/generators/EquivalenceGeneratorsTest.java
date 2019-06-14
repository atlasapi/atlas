package org.atlasapi.equiv.generators;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceGeneratorsTest {
    @Mock
    private EquivalenceGenerator<Content> generator;
    @Mock
    private ResultDescription resultDescription;
    @Mock
    private ScoredCandidates candidates;

    private EquivalenceGenerators<Content> generators;
    private EquivToTelescopeResult equivToTelescopeResult;

    @Before
    public void setUp() {
        Set<String> excludedUris = ImmutableSet.of("excluded");
        Set<String> excludedIds = ImmutableSet.of("ffds6w");

        generators = EquivalenceGenerators.create(
                Collections.singleton(generator),
                excludedUris,
                excludedIds
        );

        when(generator.generate(
                any(Content.class),
                any(ResultDescription.class),
                any(EquivToTelescopeResult.class))
        ).thenReturn(candidates);

        equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );
    }

    @Test
    public void generatorCreatesNoCandidatesForExcludedUri() {
        Item item = new Item();
        item.setCanonicalUri("excluded");
        List<ScoredCandidates<Content>> generated = generators.generate(
                item,
                resultDescription,
                equivToTelescopeResult
        );
        assertTrue(generated.isEmpty());
    }

    @Test
    public void generatorCreatesNoCandidatesForExcludedId() {
        SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        Long decodedExcludedId = codec.decode("ffds6w").longValue();

        Item item = new Item();
        item.setId(decodedExcludedId);
        List<ScoredCandidates<Content>> generated = generators.generate(
                item,
                resultDescription,
                equivToTelescopeResult
        );
        assertTrue(generated.isEmpty());
    }

    @Test
    public void generatorCreatesCandidatesForNonExcludedUri() {
        Item item = new Item();
        item.setCanonicalUri("notexcluded");
        generators.generate(item, resultDescription, equivToTelescopeResult);
        verify(generator).generate(item, resultDescription, equivToTelescopeResult);
    }

    @Test
    public void generatorCreatesCandidatesForNonExcludedId() {
        Item item = new Item();
        item.setId(12345678L);
        generators.generate(item, resultDescription, equivToTelescopeResult);
        verify(generator).generate(item, resultDescription, equivToTelescopeResult);
    }
}
