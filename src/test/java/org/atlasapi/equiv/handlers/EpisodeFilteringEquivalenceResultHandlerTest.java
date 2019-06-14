package org.atlasapi.equiv.handlers;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EpisodeFilteringEquivalenceResultHandlerTest {

    @SuppressWarnings("unchecked")
    private final EquivalenceResultHandler<Item> delegate = mock(EquivalenceResultHandler.class);
    private final EquivalenceSummaryStore summaryStore = mock(EquivalenceSummaryStore.class);

    private Episode subject;
    private Brand subjectContainer;

    private final List<ScoredCandidates<Item>> noScores = ImmutableList.of();
    private final ScoredCandidates<Item> emptyCombined = DefaultScoredCandidates.fromMappedEquivs(
            "test",
            ImmutableMap.<Item, Score>of()
    );

    @Before
    public void setup() {
        subject = new Episode("episode", "episodeCurie", Publisher.PA);
        subjectContainer = new Brand("brand", "brandCurie", Publisher.PA);
        subject.setContainer(subjectContainer);
    }

    @Test
    public void testFiltersItemFromNonStrongBrand() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        Container strongContainer = new Brand("pabrand", "pabrandCurie", Publisher.PA);

        EquivalenceSummary equivSummary = summary(
                subjectContainer.getCanonicalUri(),
                strongContainer
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer()
                        .getUri(), Optional.of(equivSummary))));

        Episode badEquiv = new Episode("bequiv", "bequivCurie", Publisher.PA);
        badEquiv.setParentRef(new ParentRef("weakpabrand"));

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.PA, ScoredCandidate.<Item>valueOf(badEquiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        handler.handle(results);

        verify(delegate).handle(argThat(resultWithNoStrongEquivalents()));
    }

    @Test
    public void testDoesntFilterItemFromStrongBrand() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        Container strongContainer = new Brand("bbcbrand", "bbcbrandCurie", Publisher.BBC);

        EquivalenceSummary equivSummary = summary(
                subjectContainer.getCanonicalUri(),
                strongContainer
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer()
                        .getUri(), Optional.of(equivSummary))));

        Episode goodEquiv = new Episode("gequiv", "gequivCurie", Publisher.BBC);
        goodEquiv.setContainer(strongContainer);

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.BBC, ScoredCandidate.<Item>valueOf(goodEquiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        handler.handle(results);

        verify(delegate).handle(argThat(resultWithStrongEquiv(Publisher.BBC, "gequiv")));
    }

    @Test
    public void testDoesntFilterItemFromSourceWithNoStrongBrandsWhenRelaxed() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        EquivalenceSummary equivSummary = new EquivalenceSummary(
                subject.getContainer().getUri(),
                ImmutableList.<String>of(),
                ImmutableMultimap.<Publisher, ContentRef>of()
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.fromMap(ImmutableMap.of(subject.getContainer()
                        .getUri(), equivSummary)));

        Episode ignoredEquiv = new Episode("ignoredequiv", "ignoredequiv", Publisher.C4);
        ignoredEquiv.setParentRef(new ParentRef("weakbutignoredbrand"));

        Multimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.C4, ScoredCandidate.<Item>valueOf(ignoredEquiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        handler.handle(results);

        verify(delegate).handle(argThat(resultWithStrongEquiv(Publisher.C4, "ignoredequiv")));
    }

    @Test
    public void testDoesntFilterItemWithNoBrand() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        EquivalenceSummary equivSummary = new EquivalenceSummary(
                subject.getCanonicalUri(),
                ImmutableList.<String>of(),
                ImmutableMultimap.<Publisher, ContentRef>of()
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer()
                        .getUri(), Optional.of(equivSummary))));

        Item noBrand = new Item("nobrand", "nobrandCurie", Publisher.FIVE);

        Multimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.FIVE, ScoredCandidate.<Item>valueOf(noBrand, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        handler.handle(results);

        verify(delegate).handle(argThat(resultWithStrongEquiv(Publisher.FIVE, "nobrand")));
    }

    @Test
    public void testFiltersItemFromSourceWithNoStrongBrandsWhenStrict() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        EquivalenceSummary equivSummary = new EquivalenceSummary(
                subject.getContainer().getUri(),
                ImmutableList.<String>of(),
                ImmutableMultimap.<Publisher, ContentRef>of()
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.fromMap(ImmutableMap.of(
                        subject.getContainer().getUri(), equivSummary
                )));

        Episode ignoredEquiv = new Episode("filteredequiv", "filteredequiv", Publisher.C4);
        ignoredEquiv.setParentRef(new ParentRef("weakbutignoredbrand"));

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.C4, ScoredCandidate.<Item>valueOf(ignoredEquiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject, noScores, emptyCombined, strong, new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.strict(
                delegate,
                summaryStore
        );

        handler.handle(results);

        verify(delegate).handle(argThat(resultWithNoStrongEquivalents()));
    }

    @Test
    public void whenContainerIsNullReturnTrueWhenDelegateReturnsTrue() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        subject.setParentRef(null);

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                ImmutableMultimap.of(Publisher.PA, ScoredCandidate.valueOf(new Item(), Score.ONE)),
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        assertThat(
                handler.handle(results),
                is(true)
        );
    }

    @Test
    public void whenContainerIsNullReturnFalseWhenDelegateReturnsFalse() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(false);

        subject.setParentRef(null);

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                ImmutableMultimap.of(Publisher.PA, ScoredCandidate.valueOf(new Item(), Score.ONE)),
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        assertThat(
                handler.handle(results),
                is(false)
        );
    }

    @Test
    public void whenThereIsNotContainerSummaryReturnFalse() {
        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.of());

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                ImmutableMultimap.of(Publisher.PA, ScoredCandidate.valueOf(new Item(), Score.ONE)),
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        try {
            handler.handle(results);
            fail("Item with missing container summary failed to throw exception");
        } catch (ContainerSummaryRequiredException e) {
            assertThat(e.getItem(), is(subject));
        }
    }

    @Test
    public void returnTrueWhenTheDelegateReturnsTrue() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(true);

        EquivalenceSummary equivSummary = summary(
                subjectContainer.getCanonicalUri(),
                new Brand("pabrand", "pabrandCurie", Publisher.PA)
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer()
                        .getUri(), Optional.of(equivSummary))));

        Episode equiv = new Episode("equiv", "curie", Publisher.PA);
        equiv.setParentRef(new ParentRef("brand"));

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.PA, ScoredCandidate.<Item>valueOf(equiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        assertThat(
                handler.handle(results),
                is(true)
        );
    }

    @Test
    public void returnFalseWhenTheDelegateReturnsFalse() {
        //noinspection unchecked
        when(delegate.handle(Matchers.any(EquivalenceResults.class)))
                .thenReturn(false);

        EquivalenceSummary equivSummary = summary(
                subjectContainer.getCanonicalUri(),
                new Brand("pabrand", "pabrandCurie", Publisher.PA)
        );

        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
                .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer()
                        .getUri(), Optional.of(equivSummary))));

        Episode equiv = new Episode("equiv", "curie", Publisher.PA);
        equiv.setParentRef(new ParentRef("brand"));

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.PA, ScoredCandidate.<Item>valueOf(equiv, Score.ONE)
        );

        EquivalenceResult<Item> result = new EquivalenceResult<Item>(
                subject,
                noScores,
                emptyCombined,
                strong,
                new DefaultDescription()
        );

        EquivalenceResults<Item> results = new EquivalenceResults<>(
                subject,
                ImmutableList.of(result),
                new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(
                delegate,
                summaryStore
        );

        assertThat(
                handler.handle(results),
                is(false)
        );
    }

    private EquivalenceSummary summary(String uri, Container strongContainer) {
        EquivalenceSummary equivSummary = new EquivalenceSummary(uri, ImmutableList.<String>of(),
                ImmutableMultimap.of(
                        strongContainer.getPublisher(),
                        ContentRef.valueOf(strongContainer)
                )
        );
        return equivSummary;
    }

    private Matcher<EquivalenceResults<Item>> resultWithStrongEquiv(
            final Publisher publisher,
            final String uri
    ) {
        return new TypeSafeMatcher<EquivalenceResults<Item>>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText("result with strong equivalent: ")
                        .appendValue(publisher)
                        .appendText("/")
                        .appendValue(uri);
            }

            @Override
            public boolean matchesSafely(EquivalenceResults<Item> result) {
                return result.strongEquivalences().stream().anyMatch(strong -> strong
                        .getCanonicalUri()
                        .equals(uri));
            }
        };
    }

    private Matcher<EquivalenceResults<Item>> resultWithNoStrongEquivalents() {
        return new TypeSafeMatcher<EquivalenceResults<Item>>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText("result with no strong equivalences");
            }

            @Override
            public boolean matchesSafely(EquivalenceResults<Item> results) {
                return results.strongEquivalences().isEmpty();
            }
        };
    }
}
