package org.atlasapi.equiv.scorers;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.hamcrest.core.IsNull;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ContainerHierarchyMatchingScorerTest {

    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private SubscriptionCatchupBrandDetector subscriptionCatchupBrandDetector = mock(SubscriptionCatchupBrandDetector.class);
    
    private final ContainerHierarchyMatchingScorer scorer = new ContainerHierarchyMatchingScorer(contentResolver, Score.negativeOne(), subscriptionCatchupBrandDetector);
    
    @Test
    @SuppressWarnings("unchecked")
    public void testScoresNullWhenFlatContainerWithChildCountsOutOfRange() {
        
        ScoredCandidates<Container> score = scorer.score(brandWithChildren(5), ImmutableSet.<Container>of(brandWithChildren(7)), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.nullScore()));
        verify(contentResolver, never()).findByCanonicalUris((Iterable<String>)any());
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testScores1WhenFlatContainerWithChildCountsInRange() {
        
        ScoredCandidates<Container> score = scorer.score(brandWithChildren(7), ImmutableSet.<Container>of(brandWithChildren(7)), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.ONE));
        verify(contentResolver, never()).findByCanonicalUris((Iterable<String>)any());
    }
    
    @Test
    public void testScoresNullWhenSeriesContainerWithSeriesCountsOutOfRange() {
        
        final Brand subject = brandWithSeries(5);
        final Brand candidate = brandWithSeries(7);

        when(contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(subject.getSeriesRefs(),SeriesRef.TO_URI))))
            .thenReturn(ResolvedContent.builder().putAll(series(5)).build());
        
        when(contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(candidate.getSeriesRefs(),SeriesRef.TO_URI))))
            .thenReturn(ResolvedContent.builder().putAll(series(7)).build());

        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.<Container>of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.nullScore()));
    }
    
    @Test
    public void testScoresOneWhenSeriesContainerWithSeriesAndEpisodeCountsInRange() {
        
        final Brand subject = brandWithSeries(5);
        final Brand candidate = brandWithSeries(6);

        when(contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(subject.getSeriesRefs(),SeriesRef.TO_URI))))
            .thenReturn(ResolvedContent.builder().putAll(series(5)).build());
        when(contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(candidate.getSeriesRefs(),SeriesRef.TO_URI))))
            .thenReturn(ResolvedContent.builder().putAll(series(6)).build());

        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.<Container>of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.ONE));
    }
    
    @Test
    public void testScoreSeriesEpisodeCounts() {
        
        ResultDescription desc = desc();
        assertThat(scorer.scoreSortedSeriesSizes(list(1,2,3,4), list(1,2,3,4), "candidate", desc), is(Score.ONE));
        assertThat(scorer.scoreSortedSeriesSizes(list(1,2,3,4,5), list(1,2,3,4), "candidate", desc), is(Score.ONE));
        assertThat(scorer.scoreSortedSeriesSizes(list(1,2,3,4), list(1,3,4), "candidate", desc), is(Score.ONE));
        assertThat(scorer.scoreSortedSeriesSizes(list(1,2,3,6), list(1,3,5), "candidate", desc), is(Score.ONE));
        assertThat(scorer.scoreSortedSeriesSizes(list(12,12,12,13), list(3,12,12,12,13), "candidate", desc), is(Score.ONE));
        
        
        assertThat(scorer.scoreSortedSeriesSizes(list(6,6), list(6,24,24,24), "candidate", desc), is(Score.negativeOne()));
        assertThat(scorer.scoreSortedSeriesSizes(list(6,6), list(24,24), "candidate", desc), is(Score.nullScore()));
        assertThat(scorer.scoreSortedSeriesSizes(list(2,3,4), list(3,4,6,7), "candidate", desc), is(Score.nullScore()));
        
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testScoresMismatchScoreWhenCandidateHasNoSeries() {
        
        Brand subject = brandWithSeries(1);
        Brand candidate = brandWithSeries(0);
        
        when(contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(subject.getSeriesRefs(),SeriesRef.TO_URI))))
            .thenReturn(ResolvedContent.builder().putAll(series(1)).build());
        
        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.negativeOne()));
        verify(contentResolver).findByCanonicalUris((Iterable<String>)any());
        
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testScoresNullWhenSubjectHasNoSeries() {
        
        Brand subject = brandWithSeries(0);
        Brand candidate = brandWithSeries(1);

        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.nullScore()));
        verify(contentResolver, never()).findByCanonicalUris((Iterable<String>)any());
        
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testScoresNullWhenFlatCandidateHasNoEpisodes() {
        
        Brand subject = brandWithChildren(1);
        Brand candidate = brandWithChildren(0);

        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.nullScore()));
        verify(contentResolver, never()).findByCanonicalUris((Iterable<String>)any());
        
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testScoresNullWhenFlatSubjectHasNoEpisodes() {
        
        Brand subject = brandWithChildren(0);
        Brand candidate = brandWithChildren(1);
        
        ScoredCandidates<Container> score = scorer.score(subject, ImmutableSet.of(candidate), desc());
        
        assertThat(Iterables.getOnlyElement(score.candidates().values()), is(Score.nullScore()));
        verify(contentResolver, never()).findByCanonicalUris((Iterable<String>)any());
        
    }

    private DefaultDescription desc() {
        return new DefaultDescription();
    }

    private List<Integer> list(Integer...is) {
        return ImmutableList.copyOf(is);
    }

    private Map<String, ? extends Identified> series(int seriesCount) {
        Builder<String, Series> builder = ImmutableMap.builder();
        for (int j = 0; j < seriesCount; j++) {
            Series series = series("uri"+j, null);
            builder.put(series.getCanonicalUri(), series);
        };
        return builder.build();
    }
    
    private Brand brandWithSeries(int series) {
        Brand brand = new Brand();
        brand.setSeriesRefs(Iterables.limit(Iterables.cycle(new SeriesRef(1234L, "uri", "sk", 1, new DateTime(DateTimeZones.UTC))), series));
        return brand;
    }

    private Brand brandWithChildren(int children) {
        Brand brand = new Brand();
        setChildren(children, brand);
        return brand;
    }

    public void setChildren(int children, Container brand) {
        brand.setChildRefs(Iterables.limit(Iterables.cycle(new ChildRef(1234L, "uri", "sk", new DateTime(DateTimeZones.UTC), EntityType.EPISODE)), children));
    }
    
    private Series series(String uri, Integer seriesNumber) {
        Series series = new Series(uri, null, Publisher.METABROADCAST);
        return series.withSeriesNumber(seriesNumber);
    }

}
