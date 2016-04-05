package org.atlasapi.remotesite.channel4.pmlsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApiClient;
import org.atlasapi.remotesite.channel4.pmlsd.C4PmlsdModule;
import org.atlasapi.remotesite.channel4.pmlsd.C4OdEpisodesAdapter;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.metabroadcast.common.http.FixedResponseHttpClient;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

@RunWith( MockitoJUnitRunner.class )
public class C4FourOdEpisodesExtractorTest extends TestCase {

    private static final long PLAYER_ID = 1;
    private static final long SERVICE_ID = 2;

    private final C4LocationPolicyIds locationPolicyIds = 
            C4LocationPolicyIds.builder()
                .withPlayerId(PLAYER_ID)
                .withWebServiceId(SERVICE_ID)
                .build();
    
    private ContentFactory<Feed, Feed, Entry> contentFactory
        = new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor());; 
    
    private String fileContentsFromResource(String resourceName)  {
        try {
            return Files.toString(new File(Resources.getResource(getClass(), resourceName).getFile()), Charsets.UTF_8);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return null;
    }
    
    private final SimpleHttpClient httpClient = new FixedResponseHttpClient(
            ImmutableMap.<String, String>of("https://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares/4od.atom", fileContentsFromResource("ramsays-kitchen-nightmares-4od.atom")));
    private final C4AtomApiClient atomApiClient = new C4AtomApiClient(httpClient, "https://pmlsc.channel4.com/pmlsd/", Optional.<String>absent());

    @Test
	public void testExtractingEpisodes() throws Exception {
		
		List<Episode> episodes = new C4OdEpisodesAdapter(atomApiClient, Optional.<Platform>absent(), 
		                                contentFactory, Publisher.C4_PMLSD, locationPolicyIds, false, new SystemClock())
		    .fetch("http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares");

		Episode firstEpisode = (Episode) Iterables.get(episodes, 0);
		
		assertThat(firstEpisode.getCanonicalUri(), is("http://pmlsc.channel4.com/pmlsd/36423/001"));
        // TODO new alias
		assertThat(firstEpisode.getAliasUrls(), hasItems(
		    "http://www.channel4.com/programmes/ramsays-kitchen-nightmares/4od#2922045",
		    "http://pmlsc.channel4.com/pmlsd/ramsays-kitchen-nightmares/episode-guide/series-1/episode-1"
	    ));
		
		assertThat(firstEpisode.getTitle(), is("Ramsay's Kitchen Nightmares"));
		assertThat(firstEpisode.getPublisher(), is(C4PmlsdModule.SOURCE));
		assertThat(firstEpisode.getSeriesNumber(), is(1));
		assertThat(firstEpisode.getEpisodeNumber(), is(1));
		assertThat(firstEpisode.getDescription(), startsWith("Gordon Ramsay visits Bonapartes in Silsden, West Yorkshire."));
		assertThat(firstEpisode.getThumbnail(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-1/ramsays-kitchen-nightmares-s1-20090617160732_200x113.jpg"));
		assertThat(firstEpisode.getImage(), is("http://www.channel4.com/assets/programmes/images/ramsays-kitchen-nightmares/series-1/ramsays-kitchen-nightmares-s1-20090617160732_625x352.jpg"));
		assertThat(firstEpisode.getAliases(), is((Set<Alias>) ImmutableSet.of(new Alias("gb:channel4:prod:pmlsd:programmeId", "36423/001"))));

		Version firstEpisodeVersion = Iterables.get(firstEpisode.getVersions(), 0);
		assertThat(firstEpisodeVersion.getDuration(), is((48 * 60) + 55));
		
		Restriction r = firstEpisodeVersion.getRestriction();
		assertThat(r.isRestricted(), is(true));
		assertThat(r.getMessage(), is("Strong language throughout"));
		assertThat(r.getMinimumAge(), is(16));
		
		assertThat(firstEpisodeVersion.getBroadcasts(), is(Collections.<Broadcast>emptySet()));
		
		Encoding firstEpsiodeEncoding = Iterables.get(firstEpisodeVersion.getManifestedAs(), 0); 
		
		Location firstEpsiodeLocation = Iterables.get(firstEpsiodeEncoding.getAvailableAt(), 0); 
		
		assertThat(firstEpsiodeLocation.getUri(), is("http://www.channel4.com/programmes/ramsays-kitchen-nightmares/4od#2922045"));
		// TODO new alias
		assertThat(firstEpsiodeLocation.getAliasUrls(), is((Set<String>) ImmutableSet.of("tag:www.channel4.com,2009:/programmes/ramsays-kitchen-nightmares/4od%232922045")));
		assertThat(firstEpsiodeLocation.getTransportType(), is(TransportType.LINK));
		
		Policy firstEpisodePolicy = firstEpsiodeLocation.getPolicy();
		assertThat(firstEpisodePolicy.getAvailabilityStart().withZone(DateTimeZones.UTC), is(new DateTime("2009-07-01T22:00:00.000Z").withZone(DateTimeZones.UTC)));
		assertThat(firstEpisodePolicy.getAvailabilityEnd().withZone(DateTimeZones.UTC), is(new DateTime("2012-12-31T00:00:00.000Z").withZone(DateTimeZones.UTC)));
		assertThat(firstEpisodePolicy.getAvailableCountries(), is((Set<Country>) Sets.newHashSet(
                Countries.fromCode("GG"),
                Countries.GB,
                Countries.fromCode("JE"),
                Countries.fromCode("IM"),
                Countries.IE)));
		
		Episode episodeWithABroadcast = (Episode) Iterables.get(episodes, 4);
		Version episodeWithABroadcastVersion = Iterables.get(episodeWithABroadcast.getVersions(), 0);
		Location locationWithBroadcast = Iterables.get(Iterables.get(episodeWithABroadcastVersion.getManifestedAs(), 0).getAvailableAt(), 0);
		
		assertThat(locationWithBroadcast.getPolicy().getAvailabilityStart(), is(new DateTime("2009-06-10T23:05:00.000Z")));
	}
}
