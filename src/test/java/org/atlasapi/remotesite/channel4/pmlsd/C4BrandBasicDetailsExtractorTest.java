package org.atlasapi.remotesite.channel4.pmlsd;

import com.google.common.io.Resources;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import junit.framework.TestCase;
import org.atlasapi.genres.AtlasGenre;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@RunWith(MockitoJUnitRunner.class)
public class C4BrandBasicDetailsExtractorTest extends TestCase {

    private final Clock clock = new TimeMachine(new DateTime(DateTimeZones.UTC));
    private final ContentFactory<Feed, Feed, Entry> contentFactory 
        = new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor());;
    
    private C4BrandBasicDetailsExtractor extractor;
	
    @Before
    public void setUp() {
        extractor = new C4BrandBasicDetailsExtractor(new C4AtomApi(new C4DummyChannelResolver()), 
                contentFactory,
                clock);
    }

    @Test
	public void testExtractingABrand() throws Exception {
		
		AtomFeedBuilder brandFeed = new AtomFeedBuilder(Resources.getResource(getClass(), "ramsays-kitchen-nightmares.atom"));
		
		Brand brand = extractor.extract(brandFeed.build());
		
		assertThat(brand.getCanonicalUri(), is("http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares"));
		// TODO new alias
		assertThat(brand.getAliasUrls(), hasItems(
	        "tag:pmlsc.channel4.com,2009:/programmes/ramsays-kitchen-nightmares",
	        "http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares"
        ));
		assertThat(brand.getTitle(), is("Ramsay's Kitchen Nightmares"));
		assertThat(brand.getLastUpdated(), is(clock.now()));
		assertThat(brand.getPublisher(), is(C4PmlsdModule.SOURCE));
		assertThat(brand.getDescription(), startsWith("Gordon Ramsay attempts to transform struggling restaurants with his"));
		assertThat(brand.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/ramsays-kitchen-nightmares_200x113.jpg"));
		assertThat(brand.getImage(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/ramsays-kitchen-nightmares_625x352.jpg"));
		assertThat(brand.getGenres(), hasItems(
		        "http://www.channel4.com/programmes/categories/food",
		        AtlasGenre.LIFESTYLE.getUri()
		));
	}
    
    @Test
    public void testExtractingXboxPlatformBrand() throws Exception {
        
        AtomFeedBuilder brandFeed = new AtomFeedBuilder(Resources.getResource(getClass(), "brasseye-xbox.atom"));
        
        Brand brand = extractor.extract(brandFeed.build());
        
        assertThat(brand.getCanonicalUri(), is("http://pmlsc.channel4.com/pmlsd/brass-eye"));
        assertThat(brand.getAliasUrls(), hasItems(
            "tag:pmlsc.channel4.com,2009:/programmes/brass-eye",
            "http://pmlsc.channel4.com/pmlsd/brass-eye"
        ));
        assertThat(brand.getTitle(), is("Brass Eye"));
        assertThat(brand.getLastUpdated(), is(clock.now()));
        assertThat(brand.getPublisher(), is(C4PmlsdModule.SOURCE));
        assertThat(brand.getDescription(), startsWith("Anarchic spoof news comedy, fronted by Chris Morris"));
        assertThat(brand.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/brass-eye/brass-eye_200x113.jpg"));
        assertThat(brand.getImage(), is("http://www.channel4.com/assets/programmes/images/brass-eye/brass-eye_625x352.jpg"));
        assertThat(brand.getGenres(), hasItems(
                "http://www.channel4.com/programmes/categories/comedy",
                AtlasGenre.COMEDY.getUri()
        ));
    }

    @Test
	public void testThatNonBrandPagesAreRejected() throws Exception {
		checkIllegalArgument("an id");
		checkIllegalArgument("tag:www.channel4.com,2009:/programmes/ramsays-kitchen-nightmares/video");
		checkIllegalArgument("tag:www.channel4.com,2009:/programmes/ramsays-kitchen-nightmares/episode-guide");
		checkIllegalArgument("tag:www.channel4.com,2009:/programmes/ramsays-kitchen-nightmares/episode-guide/series-1");
	}

	private void checkIllegalArgument(String feedId) {
		Feed feed = new Feed();
		feed.setId(feedId);
		try {
			extractor.extract(feed);
			fail("ID " + feedId + " should not be accepted");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), startsWith("Not a brand feed"));
		}
	}
}
