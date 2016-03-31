package org.atlasapi.remotesite.channel4.pmlsd.epg;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import com.metabroadcast.common.http.FixedResponseHttpClient;

import org.atlasapi.remotesite.channel4.pmlsd.epg.C4EpgClient;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgMedia;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.time.DateTimeZones;

public class C4EpgClientTest {

    @Test
    public void testGet() throws Exception {
        
        String uri = "testUri";
        String data = Resources.toString(Resources.getResource("c4-id-epg.atom"), Charsets.UTF_8);

        C4EpgClient epgClient = new C4EpgClient(new FixedResponseHttpClient(uri, data));
        
        List<C4EpgEntry> list = epgClient.get(uri);
        
        assertIsUnlinkedEntry(list.get(0));
        assertIsLinkedEntry(list.get(1));
        
    }
    
    private void assertIsUnlinkedEntry(C4EpgEntry entry) {
        assertEquals("tag:pmlsc.channel4.com,2009:slot/26424438", entry.id());
        assertEquals("The Treacle People", entry.title());
        assertTrue(entry.summary().startsWith("One Flu Over the Boggart's Nest"));
        assertEquals(new DateTime("2012-05-08T14:27:26.474Z", DateTimeZones.UTC), entry.updated());
        assertEquals(new DateTime("2012-04-26T05:05:00.000Z", DateTimeZones.UTC), entry.txDate());
        assertEquals("C4", entry.txChannel());
        assertEquals(true, entry.subtitles());
        assertEquals(false, entry.audioDescription());
        assertEquals(Duration.standardMinutes(10), entry.duration());
        assertEquals(false, entry.wideScreen());
        assertEquals(false, entry.signing());
        assertEquals(true, entry.repeat());
        assertEquals("40635/014", entry.programmeId());
        assertEquals(true, entry.simulcastRights());
        assertFalse(entry.hasRelatedLink());
    }

    private void assertIsLinkedEntry(C4EpgEntry entry) {
        assertEquals("tag:pmlsc.channel4.com,2009:slot/26424439", entry.id());
        assertEquals("Hello", entry.title());
        assertTrue(entry.summary().startsWith("Groove thinks there can't be a better way"));
        assertEquals(new DateTime("2010-11-03T05:57:50.175Z", DateTimeZones.UTC), entry.updated());
        assertEquals(new DateTime("2012-04-26T05:15:00.000Z", DateTimeZones.UTC), entry.txDate());
        assertEquals("C4", entry.txChannel());
        assertEquals(true, entry.subtitles());
        assertEquals(false, entry.audioDescription());
        assertEquals(Duration.standardSeconds(1455), entry.duration());
        assertNull(entry.wideScreen());
        assertNull(entry.signing());
        assertNull(entry.repeat());
        assertEquals("30630/003", entry.programmeId());
        assertEquals(true, entry.simulcastRights());
        
        assertTrue(entry.hasRelatedLink());
        assertEquals("http://www.channel4.com/programmes/the-hoobs/4od#2924127", entry.links().get(0).getTarget());
        assertEquals("alternate", entry.links().get(0).getRelationship());
        assertEquals("http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1/episode-3.atom", entry.links().get(1).getTarget());
        assertEquals("related", entry.links().get(1).getRelationship());
        
        C4EpgMedia media = entry.media();
        assertEquals("http://www.channel4.com/programmes/the-hoobs/4od#2924127", media.player());
        assertEquals("http://cache.channel4.com/assets/programmes/images/the-hoobs/series-1/the-hoobs-s1-20090623112301_200x113.jpg", media.thumbnail());
        assertEquals("nonadult", media.rating());
        Set<Country> expectedCountries = ImmutableSet.of(
                Countries.fromCode("IM"),
                Countries.fromCode("IE"),
                Countries.fromCode("GB"),
                Countries.fromCode("JE"),
                Countries.fromCode("GG"));

        assertEquals(expectedCountries, media.availableCountries());
    }

}
