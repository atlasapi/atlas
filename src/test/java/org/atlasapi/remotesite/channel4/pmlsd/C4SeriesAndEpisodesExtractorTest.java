package org.atlasapi.remotesite.channel4.pmlsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.io.Resources;
import com.metabroadcast.common.time.SystemClock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

@RunWith( MockitoJUnitRunner.class )
public class C4SeriesAndEpisodesExtractorTest {

    private ContentFactory<Feed, Feed, Entry> contentFactory 
        = new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor());;
    
	private final AtomFeedBuilder seriesFeed = new AtomFeedBuilder(Resources.getResource(getClass(), "ramsays-kitchen-nightmares-series-3.atom"));
	
	@Test
	public void testParsingASeries() throws Exception {
		
		SetMultimap<Series, Episode> seriesAndEpisodes = new C4SeriesAndEpisodesExtractor(contentFactory, new SystemClock())
		    .extract(seriesFeed.build());
		Series series = Iterables.getOnlyElement(seriesAndEpisodes.keySet());
		
		assertThat(series.getCanonicalUri(), is("http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares/episode-guide/series-3"));
		// TODO new alias
		assertThat(series.getAliasUrls(), hasItems(
	        "tag:pmlsc.channel4.com,2009:/programmes/ramsays-kitchen-nightmares/episode-guide/series-3",
	        "http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares/episode-guide/series-3"
        ));

		assertThat(series.getImage(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-3/ramsays-kitchen-nightmares-s3-20090617160853_625x352.jpg"));
		assertThat(series.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-3/ramsays-kitchen-nightmares-s3-20090617160853_200x113.jpg"));

		assertThat(series.getTitle(), is("Series 3 - Ramsay's Kitchen Nightmares"));
		assertThat(series.getDescription(), startsWith("Multi Michelin-starred chef Gordon Ramsay"));
		
		Map<String,Episode> episodes = Maps.uniqueIndex(seriesAndEpisodes.values(), Identified.TO_URI);
		
		Episode firstEpisode = episodes.get("http://pmlsc.channel4.com/pmlsd/41337/001");
		
		assertThat(firstEpisode.getCanonicalUri(), is("http://pmlsc.channel4.com/pmlsd/41337/001"));
		assertThat(firstEpisode.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-3/ramsays-kitchen-nightmares-s3-20090617160853_200x113.jpg"));
		assertThat(firstEpisode.getImage(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-3/ramsays-kitchen-nightmares-s3-20090617160853_625x352.jpg"));

		assertThat(series.getSeriesNumber(), is(3));
		
		assertThat(firstEpisode.getSeriesNumber(), is(3));
		assertThat(firstEpisode.getEpisodeNumber(), is(1));
		assertThat(firstEpisode.getAliases(), is((Set<Alias>) ImmutableSet.of(new Alias("gb:channel4:prod:pmlsd:programmeId", "41337/001"))));

		// since this is not a /4od feed there should be no On Demand entries
		assertThat(firstEpisode.getVersions().isEmpty(), is(true));

		// The outer adapter will notice that this is the same as the brand title (ignoring punctuation and spacing) and will replace it with the series and episode number
		assertThat(firstEpisode.getTitle(), is("Ramsay's Kitchen Nightmares"));
	}
}
