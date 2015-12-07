package org.atlasapi.equiv.scorers;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionCatchupBrandDetectorTest {

    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final SubscriptionCatchupBrandDetector detector = new SubscriptionCatchupBrandDetector(contentResolver);
    
    @Test
    public void testIdentifiesBrandWithTwoConsecutiveSeasonsAsMaybeCatchup() {
        Brand brand = new Brand();
        Series s1 = series("http://example.org/s1", 1);
        Series s2 = series("http://example.org/s2", 2);
        
        Episode s1e1 = episode("http://example.org/s1e1", 1);
        Episode s1e2 = episode("http://example.org/s1e2", 2);
        s1.setChildRefs(ImmutableList.of(s1e1.childRef(), s1e2.childRef()));
        
        Episode s2e1 = episode("http://example.org/s2e1", 1);
        Episode s2e2 = episode("http://example.org/s2e2", 2);
        s2.setChildRefs(ImmutableList.of(s2e1.childRef(), s2e2.childRef()));
        
        brand.setSeriesRefs(ImmutableList.of(s1.seriesRef(), s2.seriesRef()));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s1.getCanonicalUri(), s2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s1, s2)));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s1e1.getCanonicalUri(), s1e2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s1e1, s1e2)));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s2e1.getCanonicalUri(), s2e2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s2e1, s2e2)));
        
        assertTrue(detector.couldBeSubscriptionCatchup(brand, ImmutableList.of(s1, s2)));
    }
    
    @Test
    public void testIdentifiesBrandWithTwoNonConsecutiveSeasonsAsNotMaybeCatchup() {
        Brand brand = new Brand();
        Series s1 = series("http://example.org/s1", 1);
        Series s3 = series("http://example.org/s3", 3);
        
        brand.setSeriesRefs(ImmutableList.of(s1.seriesRef(), s3.seriesRef()));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s1.getCanonicalUri(), s3.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s1, s3)));
        
        assertFalse(detector.couldBeSubscriptionCatchup(brand, ImmutableList.of(s1, s3)));
    }
    
    @Test
    public void testIdentifiesBrandWithTwoConsecutiveSeasonsButNotConsecutiveEpisodesAsNotMaybeCatchup() {
        Brand brand = new Brand();
        Series s1 = series("http://example.org/s1", 1);
        Series s2 = series("http://example.org/s2", 2);
        
        Episode s1e1 = episode("http://example.org/s1e1", 1);
        Episode s1e2 = episode("http://example.org/s1e2", 2);
        s1.setChildRefs(ImmutableList.of(s1e1.childRef(), s1e2.childRef()));
        
        Episode s2e1 = episode("http://example.org/s2e1", 1);
        Episode s2e2 = episode("http://example.org/s2e2", 3);
       
        s2.setChildRefs(ImmutableList.of(s2e1.childRef(), s2e2.childRef()));
        
        brand.setSeriesRefs(ImmutableList.of(s1.seriesRef(), s2.seriesRef()));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s1.getCanonicalUri(), s2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s1, s2)));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s1e1.getCanonicalUri(), s1e2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s1e1, s1e2)));
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(s2e1.getCanonicalUri(), s2e2.getCanonicalUri())))
            .thenReturn(resolved(ImmutableList.of(s2e1, s2e2)));
        
        assertFalse(detector.couldBeSubscriptionCatchup(brand, ImmutableList.of(s1, s2)));
    }
    
    @Test
    // Three seasons is considered too many to be possibly subscription catchup
    public void testIdentifiesBrandWithThreeConsecutiveSeasonsAsNotMaybeCatchup() {
        Brand brand = new Brand();
        Series s1 = series("http://example.org/s1", 1);
        Series s2 = series("http://example.org/s2", 2);
        Series s3 = series("http://example.org/s3", 3);
        
        assertFalse(detector.couldBeSubscriptionCatchup(brand, ImmutableList.of(s1, s2, s3)));
    }

    @Test
    public void testIdentifiesBrandWithNoSeasonsAsNotMaybeCatchup() {
        Brand brand = new Brand();
        
        assertFalse(detector.couldBeSubscriptionCatchup(brand, ImmutableList.<Series>of()));
    }
    
    private ResolvedContent resolved(Iterable<? extends Content> contents) {
        ResolvedContentBuilder builder = ResolvedContent.builder();
        for (Content content : contents) {
            builder.put(content.getCanonicalUri(), content);
        }
        return builder.build();
    }
    
    
    public Episode episode(String uri, Integer episodeNumber) {
        Episode episode = new Episode(uri, null, Publisher.METABROADCAST);
        episode.setEpisodeNumber(episodeNumber);
        return episode;
    }
    
    public Series series(String uri, Integer seriesNumber) {
        Series series = new Series(uri, null, Publisher.METABROADCAST);
        return series.withSeriesNumber(seriesNumber);
    }

    private Brand brandWithChildren(int children) {
        Brand brand = new Brand();
        setChildren(children, brand);
        return brand;
    }
    
    public void setChildren(int children, Container brand) {
        brand.setChildRefs(Iterables.limit(Iterables.cycle(new ChildRef(1234L, "uri", "sk", new DateTime(DateTimeZones.UTC), EntityType.EPISODE)), children));
    }
}
