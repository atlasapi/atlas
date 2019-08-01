package org.atlasapi.equiv.scorers;

import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Item;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.DiscreteDomain.integers;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CrewMemberScorerTest {

    private final CrewMemberScorer scorer = new CrewMemberScorer();
    
    private final ResultDescription desc = new DefaultDescription();
    
    @Test
    public void testScoresNullWithNoCrewMembers() {
        assertThat(scoreFrom(score(itemWithCrew(), itemWithCrew())), is(Score.NULL_SCORE));
    }
    
    @Test
    public void testScoresOneWhenAllCrewMatch() {
        assertThat(scoreFrom(score(
            itemWithCrew(toCrew(namesIn('a', 'j'))), 
            itemWithCrew(toCrew(namesIn('a', 'j'))) 
        )), is(Score.ONE));
    }

    @Test
    public void testScoresNegativeWhenNoCrewMatch() {
        assertThat(scoreFrom(score(
            itemWithCrew(toCrew(namesIn('a', 'j'))), 
            itemWithCrew(toCrew(namesIn('k', 't'))) 
        )), is(Score.valueOf(-1.0)));
    }

    @Test
    public void testScoresZeroWhenThirtyPercentMatch() {
        assertThat(scoreFrom(score(
            itemWithCrew(toCrew(namesIn('a', 'j'))), 
            itemWithCrew(toCrew(namesIn('h', 'r')))
        )).asDouble(), is(closeTo(0.0, 0.1)));
    }

    private ImmutableList<Integer> namesIn(char start, char end) {
        return ContiguousSet.create(Range.closed((int)start, (int)end), integers()).asList();
    }

    private List<CrewMember> toCrew(List<Integer> numbers) {
        return Lists.transform(numbers, new Function<Integer, CrewMember>() {
            @Override
            public CrewMember apply(Integer input) {
                return crew(String.valueOf(input));
            }
        });
    }

    @Test
    public void testScoresPositiveWhenTwoOfThreeCrewMatch() {
        assertThat(scoreFrom(score(
            itemWithCrew(crew("a"), crew("b"), crew("c")), 
            itemWithCrew(crew("a"), crew("b"), crew("d"))
        )).asDouble(), is(closeTo(0.75, 0.01)));
    }

    @Test
    public void testScoresSymmetrically() {
        assertThat(scoreFrom(score(
            itemWithCrew(crew("a")), 
            itemWithCrew(crew("a"), crew("b"))
        )), is(scoreFrom(score(
            itemWithCrew(crew("a"), crew("b")),
            itemWithCrew(crew("a"))
        ))));
    }

    private CrewMember crew(String name) {
        return new CrewMember().withName(name);
    }

    protected ScoredCandidates<Item> score(Item subject, Item candidate) {
        return scorer.score(
                subject,
                ImmutableSet.of(candidate),
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
    }

    protected Score scoreFrom(ScoredCandidates<Item> scored) {
        return Iterables.getOnlyElement(scored.candidates().values());
    }

    private Item itemWithCrew(CrewMember...crew) {
        return itemWithCrew(ImmutableList.copyOf(crew));
    }

    private Item itemWithCrew(List<CrewMember> crewList) {
        Item item = new Item();
        item.setPeople(crewList);
        return item;
    }

}
