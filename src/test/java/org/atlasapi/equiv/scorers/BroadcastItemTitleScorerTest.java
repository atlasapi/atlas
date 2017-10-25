package org.atlasapi.equiv.scorers;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class BroadcastItemTitleScorerTest {

    private final Score mismatchScore = Score.nullScore();
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final BroadcastItemTitleScorer scorer
        = new BroadcastItemTitleScorer(resolver, mismatchScore);
    private final EquivToTelescopeResults equivToTelescopeResults =
            EquivToTelescopeResults.create("id", "publisher");
    
    @Test
    public void testScoresOneIfBrandTitleMatchesWhenCandidateHasContainer() {
        
        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setTitle("Coast");
        
        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setTitle("South Wales");
        
        Brand candidateBrand = new Brand("candBrand", "candBrand", Publisher.PA);
        candidateBrand.setTitle("Coast");
        candidate.setContainer(candidateBrand);
        
        when(resolver.findByCanonicalUris(argThat(hasItem(candidateBrand.getCanonicalUri()))))
            .thenReturn(ResolvedContent.builder().put(candidateBrand.getCanonicalUri(), candidateBrand).build());
        
        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                equivToTelescopeResults
        );
        
        assertThat(results.candidates().get(candidate), is(Score.ONE));
        
    }

    @Test
    public void testScoresOneIfBrandTitleMatchesWhenSubjectHasContainer() {
        
        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setTitle("South Wales");
        
        Brand subjectBrand = new Brand("subjBrand", "subjBrand", Publisher.PA);
        subjectBrand.setTitle("Coast");
        subject.setContainer(subjectBrand);
        
        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setTitle("Coast");
        
        when(resolver.findByCanonicalUris(argThat(hasItem(subjectBrand.getCanonicalUri()))))
        .thenReturn(ResolvedContent.builder().put(subjectBrand.getCanonicalUri(), subjectBrand).build());
        
        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                equivToTelescopeResults
        );
        
        assertThat(results.candidates().get(candidate), is(Score.ONE));
        
    }

    @Test
    public void testScoresOneIfItemTitleMatchesWhenCandidateHasContainer() {
        
        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setTitle("Coast");
        
        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setTitle("Coast");
        
        Brand candidateBrand = new Brand("candBrand", "candBrand", Publisher.PA);
        candidateBrand.setTitle("Coast");
        candidate.setContainer(candidateBrand);
        
        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                equivToTelescopeResults
        );
        
        assertThat(results.candidates().get(candidate), is(Score.ONE));
        
        verify(resolver, never()).findByCanonicalUris(anyIterable());
    }

    @Test
    public void testScoresMismatchScoreIfNoTitlesMatch() {
        
        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        
        Brand subjectBrand = new Brand("subjBrand", "subjBrand", Publisher.PA);
        subject.setContainer(subjectBrand);
        
        Item candidate = new Item("cand", "cand", Publisher.PA);
        
        Brand candidateBrand = new Brand("candBrand", "candBrand", Publisher.PA);
        candidate.setContainer(candidateBrand);
        
        when(resolver.findByCanonicalUris(argThat(hasItem(subjectBrand.getCanonicalUri()))))
            .thenReturn(ResolvedContent.builder().put(subjectBrand.getCanonicalUri(), subjectBrand).build());
        when(resolver.findByCanonicalUris(argThat(hasItem(candidateBrand.getCanonicalUri()))))
            .thenReturn(ResolvedContent.builder().put(candidateBrand.getCanonicalUri(), candidateBrand).build());
    
        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                equivToTelescopeResults
        );
        
        assertThat(results.candidates().get(candidate), is(mismatchScore));
        
        verify(resolver).findByCanonicalUris(argThat(hasItem(subjectBrand.getCanonicalUri())));
        verify(resolver).findByCanonicalUris(argThat(hasItem(candidateBrand.getCanonicalUri())));
    }

    @SuppressWarnings("unchecked")
    private Iterable<String> anyIterable() {
        return isA(Iterable.class);
    }

}
