package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;

import com.google.common.io.Resources;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith( MockitoJUnitRunner.class )
public class C4BrandClipExtractorTest {

    private final AtomFeedBuilder greenWingFeed = new AtomFeedBuilder(Resources.getResource(getClass(), "green-wing-video.atom"));
    private final AtomFeedBuilder hollyoaksFeed = new AtomFeedBuilder(Resources.getResource(getClass(), "hollyoaks.atom"));
    
    private final ContentFactory<Feed, Feed, Entry> contentFactory
        = new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor());
    
    private final Clock clock = new TimeMachine(new DateTime(DateTimeZones.UTC));
    private final C4LocationPolicyIds locationPolicyIds = C4LocationPolicyIds.builder().build();
    private final C4BrandClipExtractor clipExtractor = new C4BrandClipExtractor(contentFactory, Publisher.C4_PMLSD, locationPolicyIds, new C4ContentLinker(), clock);
    
    @Test
    public void testExtract() {
        
        Clip clip = clipExtractor.extract((Entry)greenWingFeed.build().getEntries().get(0));
        
        assertThat(clip.getCanonicalUri(), is("http://www.channel4.com/programmes/green-wing/video/series-1/episode-7/guyball"));
        assertThat(clip.getPublisher(), is(C4PmlsdModule.SOURCE));
        assertThat(clip.getLastUpdated(), is(clock.now()));
        assertThat(clip.getIsLongForm(), is(false));
        assertThat(clip.getMediaType(), is(MediaType.VIDEO));
        assertThat(clip.getSpecialization(), is(Specialization.TV));
        assertThat(clip.getTitle(), is("Guyball"));        
        assertThat(clip.getDescription(), is("'This country could soon have a new world champion'"));
        assertThat(clip.getImage(), is("http://www.channel4.com/assets/programmes/images/green-wing/series-1/episode-7/guyball/green-wing-s1e7-guyball_625x352.jpg"));
        assertThat(clip.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/green-wing/series-1/episode-7/guyball/green-wing-s1e7-guyball_200x113.jpg"));
        assertThat(clip.getClipOf(), is("s1-e7"));
   
    }
    
    @Test
    public void testExtractBipsUri() {
        Clip clip = clipExtractor.extract((Entry)hollyoaksFeed.build().getEntries().get(0));
        assertThat(clip.getImage(), is("http://static.bips.channel4.com/bips/625x352/videos/3838863635001.jpg"));
        assertThat(clip.getThumbnail(), is("http://static.bips.channel4.com/bips/200x113/videos/3838863635001.jpg"));
    }
    
    @Test
    public void testExtractSeriesClip() {
        Clip clip = clipExtractor.extract((Entry)greenWingFeed.build().getEntries().get(11));
        assertThat(clip.getClipOf(), is("s1"));
    }

}
