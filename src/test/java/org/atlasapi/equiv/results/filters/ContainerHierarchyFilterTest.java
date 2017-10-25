package org.atlasapi.equiv.results.filters;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ContainerHierarchyFilterTest {

    private final ContainerHierarchyFilter filter = new ContainerHierarchyFilter();
    
    @Test
    public void testApply() {
        
        Brand brand = new Brand("brand","brand",Publisher.BBC);
        Series topLevelSeries = new Series("tlSeries","tlSeries",Publisher.BBC);
        Series nonTopLevelSeries = new Series("ntlSeries","ntlSeries",Publisher.BBC);
        nonTopLevelSeries.setParent(brand);
        
        List<ScoredCandidate<Container>> candidates = candidates(brand, topLevelSeries, nonTopLevelSeries);
        checkFiltered(filter.apply(
                candidates,
                brand,
                new DefaultDescription(),
                EquivToTelescopeResults.create("id", "publisher")
        ), brand, topLevelSeries);
        checkFiltered(filter.apply(
                candidates,
                topLevelSeries,
                new DefaultDescription(),
                EquivToTelescopeResults.create("id", "publisher")
        ), brand, topLevelSeries);
        checkFiltered(filter.apply(
                candidates,
                nonTopLevelSeries,
                new DefaultDescription(),
                EquivToTelescopeResults.create("id", "publisher")
        ), nonTopLevelSeries);
        
    }

    private void checkFiltered(List<ScoredCandidate<Container>> filtered, Container...containers) {
        assertEquals(ImmutableList.copyOf(containers), Lists.transform(filtered, ScoredCandidate.<Container>toCandidate()));
    }

    private List<ScoredCandidate<Container>> candidates(Container... containers) {
        return ImmutableList.copyOf(Iterables.transform(ImmutableList.copyOf(containers),
                new Function<Container, ScoredCandidate<Container>>() {

                    @Override
                    public ScoredCandidate<Container> apply(Container input) {
                        return ScoredCandidate.valueOf(input, Score.nullScore());
                    }
                }));
    }

}
