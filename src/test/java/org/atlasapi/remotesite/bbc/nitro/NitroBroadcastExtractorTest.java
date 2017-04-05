package org.atlasapi.remotesite.bbc.nitro;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBroadcastExtractor;

import com.metabroadcast.atlas.glycerin.model.BroadcastTime;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.Ids;
import com.metabroadcast.atlas.glycerin.model.ServiceReference;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class NitroBroadcastExtractorTest {

    private static final String TERRESTRIAL_EVENT_LOCATOR_TYPE = "terrestrial_event_locator";
    private static final String TERRESTRIAL_PROGRAM_CRID_TYPE = "terrestrial_programme_crid";
    private static final String BID_TYPE = "bid";
    private static final String PIPS_AUTHORITY = "pips";
    private static final String TELEVIEW_AUTHORITY = "teleview";
    private static final String TEL_VALUE = "TEL VALUE";
    private static final String CRID_VALUE = "crid value";
    private static final String BID_VALUE = "bid value";
    private static final String ALIAS_PREFIX = "bbc";
    private static final String OTHER_AUTHORITY = "other";

    private final NitroBroadcastExtractor extractor
        = new NitroBroadcastExtractor();
    
    @Test
    public void testExtractingBroadcast() throws Exception {
        
        com.metabroadcast.atlas.glycerin.model.Broadcast nitro
            = new com.metabroadcast.atlas.glycerin.model.Broadcast();
        nitro.setPid("p01g1w71");
        
        ServiceReference service = new ServiceReference();
        service.setSid("bbc_one_london");
        nitro.setService(service);
        
        XMLGregorianCalendar start = xmlGregCal("2013-09-11T05:00:00Z");
        XMLGregorianCalendar end = xmlGregCal("2013-09-11T08:15:00Z");
        BroadcastTime publishedTime = new BroadcastTime();
        publishedTime.setStart(start);
        publishedTime.setEnd(end);
        nitro.setPublishedTime(publishedTime);
        nitro.setIsAudioDescribed(true);
        nitro.setIds(ids());
        
        nitro.setIsRepeat(true);
        nitro.setIsAudioDescribed(true);
        
        Optional<Broadcast> extracted = extractor.extract(nitro);
        Broadcast atlas = extracted.get();

        assertThat(atlas.getBroadcastOn(), is("http://www.bbc.co.uk/services/bbcone/london"));
        assertThat(
                atlas.getTransmissionTime(),
                is(new DateTime("2013-09-11T05:00:00Z", DateTimeZones.UTC))
        );
        assertThat(
                atlas.getTransmissionEndTime(),
                is(new DateTime("2013-09-11T08:15:00Z", DateTimeZones.UTC))
        );
        assertThat(atlas.getSourceId(), is("bbc:p01g1w71"));
        assertTrue(atlas.getRepeat());
        assertTrue(atlas.getAudioDescribed());
        assertThat(
                atlas.getAliases(),
                hasItem(alias(
                        TERRESTRIAL_EVENT_LOCATOR_TYPE,
                        TELEVIEW_AUTHORITY,
                        TEL_VALUE.toLowerCase()
                ))
        );
        assertThat(
                atlas.getAliases(),
                hasItem(alias(TERRESTRIAL_PROGRAM_CRID_TYPE, TELEVIEW_AUTHORITY, CRID_VALUE))
        );
        assertThat(atlas.getAliases(), hasItem(alias(BID_TYPE, PIPS_AUTHORITY, BID_VALUE)));
        assertThat(
                atlas.getAliases(),
                Matchers.not(hasItem(alias(BID_TYPE, OTHER_AUTHORITY, BID_VALUE)))
        );
    }

    private Alias alias(String type, String authority, String value) {
        return new Alias(namespace(type, authority), value);
    }

    private String namespace(String type, String authority) {
        return Joiner.on(':').join(ALIAS_PREFIX, type, authority);
    }

    private Ids ids() {
        Ids ids = new Ids();

        ids.getId().add(id(TERRESTRIAL_EVENT_LOCATOR_TYPE, TELEVIEW_AUTHORITY, TEL_VALUE));
        ids.getId().add(id(TERRESTRIAL_PROGRAM_CRID_TYPE, TELEVIEW_AUTHORITY, CRID_VALUE));
        ids.getId().add(id(BID_TYPE, PIPS_AUTHORITY, BID_VALUE));

        return ids;
    }

    private Id id(String type, String authority, String value) {
        Id id = new Id();

        id.setType(type);
        id.setAuthority(authority);
        id.setValue(value);

        return id;
    }

    private XMLGregorianCalendar xmlGregCal(String lexicalRepresentation)
            throws DatatypeConfigurationException {
        return DatatypeFactory.newInstance().newXMLGregorianCalendar(lexicalRepresentation);
    }

}
