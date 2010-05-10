package org.uriplay.remotesite.bbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.List;
import java.util.Set;

import org.jmock.integration.junit3.MockObjectTestCase;
import org.joda.time.DateTime;
import org.uriplay.media.entity.Broadcast;
import org.uriplay.remotesite.bbc.SlashProgrammesVersionRdf.BbcBroadcast;
import org.uriplay.remotesite.bbc.SlashProgrammesVersionRdf.BroadcastOn;
import org.uriplay.remotesite.bbc.SlashProgrammesVersionRdf.Interval;

import com.google.common.collect.Lists;


/**
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class BbcProgrammeGraphExtractorTest extends MockObjectTestCase {

	public void testGeneratesSetOfBbcAliasUrisForIplayerEpisode() {
		Set<String> aliases = BbcProgrammeGraphExtractor.bbcAliasUrisFor("http://www.bbc.co.uk/iplayer/episode/b00nxb4q/Later_Live..._with_Jools_Holland_Series_35_Episode_9/");
		assertThat(aliases, hasItem("http://www.bbc.co.uk/programmes/b00nxb4q"));
		assertThat(aliases, hasItem("http://bbc.co.uk/i/nxb4q/"));
		assertThat(aliases, not(hasItem("http://www.bbc.co.uk/iplayer/episode/b00nxb4q/Later_Live..._with_Jools_Holland_Series_35_Episode_9/")));
	}
	
	public void testGeneratesSetOfBbcAliasUrisForSlashProgrammesEpisode() {
		Set<String> aliases = BbcProgrammeGraphExtractor.bbcAliasUrisFor("http://www.bbc.co.uk/programmes/b00nxb4q");
		assertThat(aliases, hasItem("http://bbc.co.uk/i/nxb4q/"));
		assertThat(aliases, hasItem("http://www.bbc.co.uk/iplayer/episode/b00nxb4q"));
		assertThat(aliases, not(hasItem("http://www.bbc.co.uk/programmes/b00nxb4q")));
	}
	
	public void testReturnsEmptySetForUriThatDoesNotContainPid() {
		Set<String> aliases = BbcProgrammeGraphExtractor.bbcAliasUrisFor("http://www.bbc.co.uk/programmes/today");
		assertTrue(aliases.isEmpty());
	}
	
	public void testThatDuplicateBroadcastsAreIgnored() throws Exception {
		
		DateTime t1 = new DateTime();
		DateTime t2 = t1.plusHours(1);
		DateTime t3 = t2.plusHours(1);
		
		System.out.println( broadcast("/bbc2", t1, t2).equals( broadcast("/bbc2", t1, t2)));
		
		List<BbcBroadcast> bbcBroadcasts = Lists.newArrayList(broadcast("/bbc1", t1, t2), broadcast("/bbc2", t1, t2), broadcast("/bbc1", t1, t2), broadcast("/bbc2", t1, t3));
		
		Set<Broadcast> broadcasts = BbcProgrammeGraphExtractor.broadcastsFrom(versionWithBroadcasts(bbcBroadcasts));
		
		// broadcast("/bbc1", t1, t2) should be removed
		assertThat(broadcasts.size(), is(3));
	}

	private BbcBroadcast broadcast(String channel, DateTime start, DateTime end) {
		BbcBroadcast broadcast = new BbcBroadcast();
		broadcast.broadcastOn = new BroadcastOn();
		broadcast.broadcastOn.resourceUri = channel;
		Interval interval = new Interval();
		interval.startTime = start.toString();
		interval.endTime = end.toString();
		broadcast.atTime(interval);
		return broadcast;
	}


	private SlashProgrammesVersionRdf versionWithBroadcasts(List<BbcBroadcast> broadcasts) {
		SlashProgrammesVersionRdf version = new SlashProgrammesVersionRdf();
		version.firstBroadcasts = broadcasts;
		return version;
	}
}
