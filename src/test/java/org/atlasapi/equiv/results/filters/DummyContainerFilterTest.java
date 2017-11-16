package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Test;

public class DummyContainerFilterTest {

    private DummyContainerFilter dummyContainerFilter = new DummyContainerFilter();
    private final EquivToTelescopeResults equivToTelescopeResults =
            EquivToTelescopeResults.create("id", "publisher");

    @Test
    public void testDoesntFilterNonContainers() {
        Container subject = new Brand();
        Item candidate = new Item();
        candidate.setId(1L);
        ResultDescription result = new DefaultDescription();

        assertTrue(
                dummyContainerFilter.doFilter(
                        ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject,
                        result,
                        equivToTelescopeResults
                )
        );
        assertTrue(
                dummyContainerFilter.doFilter(
                        ScoredCandidate.valueOf(candidate, Score.ONE),
                        candidate,
                        result,
                        equivToTelescopeResults
                )
        );
    }

    @Test
    public void testDoesntFilterNotDummy() {
        Container subject = new Brand();
        ChildRef ref = new ChildRef(1l, "uri", "key", DateTime.now(),
                EntityType.EPISODE
        );
        subject.setChildRefs(ImmutableSet.of(ref));
        Container candidate = new Container();
        candidate.setId(1L);
        candidate.setChildRefs(ImmutableSet.of(ref));
        ResultDescription result = new DefaultDescription();

        assertTrue(
                dummyContainerFilter.doFilter(
                        ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject,
                        result,
                        equivToTelescopeResults
                )
        );
    }

    @Test
    public void testFiltersDummyBrand() {
        Container subject = new Brand();
        Container candidate = new Brand();
        ResultDescription result = new DefaultDescription();

        assertFalse(
                dummyContainerFilter.doFilter(
                        ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject,
                        result,
                        equivToTelescopeResults
                )
        );
    }

    @Test
    public void testFiltersDummySeries() {
        Container subject = new Series();
        Container candidate = new Series();
        candidate.setPeople(ImmutableList.of(new CrewMember()));
        ResultDescription result = new DefaultDescription();

        assertFalse(
                dummyContainerFilter.doFilter(
                        ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject,
                        result,
                        equivToTelescopeResults
                )
        );
    }

}