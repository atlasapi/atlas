package org.atlasapi.equiv.update;

import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.results.EquivalenceResult;
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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.collect.ImmutableOptionalMap;

@RunWith(MockitoJUnitRunner.class)
public class EpisodeFilteringEquivalenceResultHandlerTest {

    @SuppressWarnings("unchecked")
    private final EquivalenceResultHandler<Item> delegate = mock(EquivalenceResultHandler.class);
    private final EquivalenceSummaryStore summaryStore = mock(EquivalenceSummaryStore.class);
    
    private Episode subject;
    private Brand subjectContainer;
    
    private final List<ScoredCandidates<Item>> noScores = ImmutableList.of();
    private final ScoredCandidates<Item> emptyCombined = DefaultScoredCandidates.fromMappedEquivs("test", ImmutableMap.<Item, Score>of());
    
    @Before
    public void setup() {
        subject = new Episode("episode","episodeCurie", Publisher.PA);
        subjectContainer = new Brand("brand", "brandCurie", Publisher.PA);
        subject.setContainer(subjectContainer);
    }

    private Matcher<EquivalenceResult<Item>> resultWithStrongEquiv(final Publisher publisher, final String uri) {
        return new TypeSafeMatcher<EquivalenceResult<Item>>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText("result with strong equivalent: ")
                    .appendValue(publisher)
                    .appendText("/")
                    .appendValue(uri);
            }

            @Override
            public boolean matchesSafely(EquivalenceResult<Item> result) {
                return result.strongEquivalences().get(publisher).stream().anyMatch(strong -> strong.candidate().getCanonicalUri().equals(uri));
            }
        };
    }

    private Matcher<EquivalenceResult<Item>> resultWithNoStrongEquivalents() {
        return new TypeSafeMatcher<EquivalenceResult<Item>>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText("result with no strong equivalences");
            }

            @Override
            public boolean matchesSafely(EquivalenceResult<Item> result) {
                return result.strongEquivalences().isEmpty();
            }
        };
    }
    
    @Test
    public void testFiltersItemFromNonStrongBrand() {
        
        Container strongContainer = new Brand("pabrand", "pabrandCurie", Publisher.PA);

        EquivalenceSummary equivSummary = summary(subjectContainer.getCanonicalUri(), strongContainer);
        
        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
            .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer().getUri(), Optional.of(equivSummary))));
        
        Episode badEquiv = new Episode("bequiv", "bequivCurie", Publisher.PA);
        badEquiv.setParentRef(new ParentRef("weakpabrand"));

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
            Publisher.PA, ScoredCandidate.<Item>valueOf(badEquiv, Score.ONE)
        );
        
        EquivalenceResult<Item> result = new EquivalenceResult<Item>(subject, noScores, emptyCombined , strong, new DefaultDescription());

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(delegate, summaryStore);
        
        handler.handle(result, null, null);
        
        verify(delegate).handle(argThat(
                resultWithNoStrongEquivalents()),
                any(),
                any(IngestTelescopeClientImpl.class)
        );
    }
    
    
    @Test
    public void testDoesntFilterItemFromStrongBrand() {
        
        Container strongContainer = new Brand("bbcbrand", "bbcbrandCurie", Publisher.BBC);

        EquivalenceSummary equivSummary = summary(subjectContainer.getCanonicalUri(), strongContainer);
        
        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
            .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer().getUri(), Optional.of(equivSummary))));
        
        Episode goodEquiv = new Episode("gequiv", "gequivCurie", Publisher.BBC);
        goodEquiv.setContainer(strongContainer);

        ImmutableMultimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
            Publisher.BBC, ScoredCandidate.<Item>valueOf(goodEquiv, Score.ONE)
        );
        
        EquivalenceResult<Item> result = new EquivalenceResult<Item>(subject, noScores, emptyCombined , strong, new DefaultDescription());
        
        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(delegate, summaryStore);
        
        handler.handle(result, null, null);

        verify(delegate).handle(argThat(
                resultWithStrongEquiv(Publisher.BBC, "gequiv")),
                any(),
                any(IngestTelescopeClientImpl.class)
        );
    }
    
    @Test
    public void testDoesntFilterItemFromSourceWithNoStrongBrandsWhenRelaxed() {

        EquivalenceSummary equivSummary = new EquivalenceSummary(subject.getContainer().getUri(), ImmutableList.<String>of(), ImmutableMultimap.<Publisher,ContentRef>of());
        
        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
            .thenReturn(ImmutableOptionalMap.fromMap(ImmutableMap.of(subject.getContainer().getUri(), equivSummary)));
        
        Episode ignoredEquiv = new Episode("ignoredequiv", "ignoredequiv", Publisher.C4);
        ignoredEquiv.setParentRef(new ParentRef("weakbutignoredbrand"));

        Multimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
            Publisher.C4, ScoredCandidate.<Item>valueOf(ignoredEquiv, Score.ONE)
        );
        
        EquivalenceResult<Item> result = new EquivalenceResult<Item>(subject, noScores, emptyCombined , strong, new DefaultDescription());

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(delegate, summaryStore);
        
        handler.handle(result, null, null);

        verify(delegate).handle(argThat(
                resultWithStrongEquiv(Publisher.C4, "ignoredequiv")),
                any(),
                any(IngestTelescopeClientImpl.class)
        );
    }
    
    @Test
    public void testDoesntFilterItemWithNoBrand() {
        
        EquivalenceSummary equivSummary = new EquivalenceSummary(subject.getCanonicalUri(), ImmutableList.<String>of(), ImmutableMultimap.<Publisher,ContentRef>of());
        
        when(summaryStore.summariesForUris(argThat(hasItem(subject.getContainer().getUri()))))
            .thenReturn(ImmutableOptionalMap.copyOf(ImmutableMap.of(subject.getContainer().getUri(), Optional.of(equivSummary))));
        
        Item noBrand = new Item("nobrand", "nobrandCurie", Publisher.FIVE);

        Multimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
                Publisher.FIVE, ScoredCandidate.<Item>valueOf(noBrand, Score.ONE)
        );
        
        EquivalenceResult<Item> result = new EquivalenceResult<Item>(subject, noScores, emptyCombined , strong, new DefaultDescription());

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.relaxed(delegate, summaryStore);
        
        handler.handle(result, null, null);
        
        verify(delegate).handle(argThat(
                resultWithStrongEquiv(Publisher.FIVE, "nobrand")),
                any(),
                any(IngestTelescopeClientImpl.class)
        );
    }
    
    @Test
    public void testFiltersItemFromSourceWithNoStrongBrandsWhenStrict() {
        
        EquivalenceSummary equivSummary = new EquivalenceSummary(
            subject.getContainer().getUri(), 
            ImmutableList.<String>of(), 
            ImmutableMultimap.<Publisher,ContentRef>of()
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
            subject, noScores, emptyCombined , strong, new DefaultDescription()
        );

        EquivalenceResultHandler<Item> handler = EpisodeFilteringEquivalenceResultHandler.strict(delegate, summaryStore);
        
        handler.handle(result, null, null);

        verify(delegate).handle(
                argThat(resultWithNoStrongEquivalents()),
                any(),
                any(IngestTelescopeClientImpl.class)
        );
    }
    

    private EquivalenceSummary summary(String uri, Container strongContainer) {
        EquivalenceSummary equivSummary = new EquivalenceSummary(uri, ImmutableList.<String>of(),
                ImmutableMultimap.of(strongContainer.getPublisher(), ContentRef.valueOf(strongContainer)));
        return equivSummary;
    }

}
