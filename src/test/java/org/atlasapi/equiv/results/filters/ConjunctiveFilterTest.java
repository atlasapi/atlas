package org.atlasapi.equiv.results.filters;

import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConjunctiveFilterTest {

    @Test
    public void test() {
        EquivalenceFilter<Integer> filter = ConjunctiveFilter.valueOf(ImmutableList.of(
            MultiplesFilter.of(3),
            MultiplesFilter.of(2),
            AlwaysTrueFilter.<Integer>get()
        ));
        
        DefaultDescription desc = new DefaultDescription();
        Iterable<ScoredCandidate<Integer>> candidates = candidatesFor(Range.closed(0, 20));
        List<ScoredCandidate<Integer>> filtered = filter.apply(
                candidates,
                null,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        
        assertThat(Lists.transform(filtered, ScoredCandidate.<Integer>toCandidate()), is(hasItems(6,12,18)));
    }

    private Iterable<ScoredCandidate<Integer>> candidatesFor(Range<Integer> range) {
        return Iterables.transform(ContiguousSet.create(range, DiscreteDomain.integers()), new Function<Integer, ScoredCandidate<Integer>>() {
            @Override
            public ScoredCandidate<Integer> apply(@Nullable Integer input) {
                return ScoredCandidate.valueOf(input, Score.NULL_SCORE);
            }
        });
    }

    public static class MultiplesFilter extends AbstractEquivalenceFilter<Integer> {

        public static final EquivalenceFilter<Integer> of(int factor) {
            return new MultiplesFilter(factor);
        }
        
        private final Integer factor;

        private MultiplesFilter(Integer factor) {
            this.factor = factor;
        }

        @Override
        protected boolean doFilter(
                ScoredCandidate<Integer> input,
                Integer subject,
                ResultDescription desc,
                EquivToTelescopeResult equivToTelescopeResult
        ) {
            return input.candidate() % factor == 0;
        }
        
    }
    
}
