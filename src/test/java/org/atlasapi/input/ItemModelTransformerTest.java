package org.atlasapi.input;

import java.math.BigInteger;
import java.util.Date;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Award;
import org.atlasapi.media.entity.simple.Broadcast;
import org.atlasapi.media.entity.simple.EventRef;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Location;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.media.entity.simple.Restriction;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ItemModelTransformerTest {

    private ItemModelTransformer transformer;

    private @Mock LookupEntryStore lookupEntryStore;
    private @Mock TopicStore topicStore;
    private @Mock ChannelResolver channelResolver;
    private @Mock NumberToShortStringCodec idCodec;
    private @Mock ClipModelTransformer clipModelTransformer;
    private @Mock Clock clock;
    private @Mock SegmentModelTransformer segmentModelTransformer;

    private Item simpleItem;
    private Broadcast simpleBroadcast;
    private Restriction simpleRestriction;

    @Before
    public void setUp() throws Exception {
        transformer = new ItemModelTransformer(
                lookupEntryStore,
                topicStore,
                channelResolver,
                idCodec,
                clipModelTransformer,
                clock,
                segmentModelTransformer
        );

        simpleRestriction = getSimpleRestriction();
        simpleBroadcast = getSimpleBroadcast();
        simpleItem = getSimpleItem();
    }

    @Test
    public void testTransformItemWithBroadcastVersionsTransformsAllVersionFields()
            throws Exception {
        simpleItem.setShortDescription("H");
        simpleItem.setMediumDescription("Hello");
        simpleItem.setLongDescription("Hello World");
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));
        assertThat(simpleItem.getShortDescription(), is(complex.getShortDescription()));
        assertThat(simpleItem.getMediumDescription(), is(complex.getMediumDescription()));
        assertThat(simpleItem.getLongDescription(), is(complex.getLongDescription()));

        Version version = complex.getVersions().iterator().next();
        checkRestriction(version.getRestriction());
    }

    @Test
    public void testSetsAdditionalFieldsForFilms() {
        Item film = getSimpleItem();
        film.setType("film");
        film.setYear(2000);
        film.addCountryOfOrigin(Countries.GB);
        org.atlasapi.media.entity.Item complex = transformer.transform(film);

        assertTrue(complex instanceof Film);
        assertThat(complex.getYear(), is(2000));
        assertTrue(complex.getCountriesOfOrigin().contains(Countries.GB));
    }


    @Test
    public void testSetsDurationFromLocation()
            throws Exception {
        simpleItem.addLocation(getSimpleLocationWithDuration(2000));
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));
        Version version = complex.getVersions().iterator().next();
        assertThat(version.getDuration(), is (2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsExceptionIfMultipleLocationsWithDifferentDurations()
            throws Exception {
        simpleItem.addLocation(getSimpleLocationWithDuration(2000));
        simpleItem.addLocation(getSimpleLocationWithDuration(3000));
        transformer.transform(simpleItem);
    }

    @Test
    public void testSetsDurationFromLocationsWithIdenticalDurations(){
        simpleItem.addLocation(getSimpleLocationWithDuration(2000));
        simpleItem.addLocation(getSimpleLocationWithDuration(2000));
        simpleItem.addLocation(getSimpleLocationWithDuration(2000));
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));
        Version version = complex.getVersions().iterator().next();
        assertThat(version.getDuration(), is (2));
    }

    @Test
    public void testNoDurationIsSetIfNoLocationProvided(){
        simpleItem.addLocation(new Location());
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));
        Version version = complex.getVersions().iterator().next();
        assertNull(version.getDuration());
    }

    @Test
    public void testBroadcastsAreTransformedForItemOfTypeBroadcast() throws Exception {
        simpleItem.setType("broadcast");
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));

        Version version = complex.getVersions().iterator().next();
        assertThat(version.getBroadcasts().size(), is(1));

        org.atlasapi.media.entity.Broadcast broadcast = version.getBroadcasts().iterator().next();
        assertThat(broadcast.getBroadcastOn(), is(simpleBroadcast.getBroadcastOn()));
    }

    @Test
    public void testTransformingItemWithBroadcastSetsRestriction() throws Exception {
        Location location = new Location();
        location.setRestriction(simpleRestriction);

        simpleItem.addLocation(location);
        simpleItem.setBroadcasts(Lists.<Broadcast>newArrayList());

        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));

        Version version = complex.getVersions().iterator().next();
        checkRestriction(version.getRestriction());
    }

    @Test
    public void testTransformingItemWithLocationsSetsRestriction() throws Exception {
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));

        Version version = complex.getVersions().iterator().next();
        checkRestriction(version.getRestriction());
    }

    @Test
    public void testTransformingItemWithAwards() {
        org.atlasapi.media.entity.Item complex = transformer.transform(getSimpleItemWithAward());
        org.atlasapi.media.entity.Award award = Iterables.getOnlyElement(complex.getAwards());
        assertEquals("title", award.getTitle());
        assertEquals("description", award.getDescription());
        assertEquals("won", award.getOutcome());
        assertEquals(2009, award.getYear().intValue());


    }

    public void testTransformItemWithEventRefs() {
        when(idCodec.decode("12345")).thenReturn(BigInteger.valueOf(12345));
        when(idCodec.decode("1234")).thenReturn(BigInteger.valueOf(1234));
        org.atlasapi.media.entity.Item complex = transformer.transform(getItemWithEventRefs());
        assertEquals(1, complex.events().size());
        assertThat(complex.events().get(0).id(), is(1234L));
    }

    private void checkRestriction(org.atlasapi.media.entity.Restriction restriction) {
        assertThat(restriction.isRestricted(), is(simpleRestriction.isRestricted()));
        assertThat(restriction.getMinimumAge(), is(simpleRestriction.getMinimumAge()));
        assertThat(restriction.getRating(), is(simpleRestriction.getRating()));
        assertThat(restriction.getAuthority(), is(simpleRestriction.getAuthority()));
        assertThat(restriction.getMessage(), is(simpleRestriction.getMessage()));
    }

    private Item getSimpleItem() {
        Item item = new Item();
        item.setUri("uri");
        item.setPublisher(new PublisherDetails(Publisher.BBC.key()));
        item.setBroadcasts(Lists.newArrayList(simpleBroadcast));
        return item;
    }

    private Item getSimpleItemWithAward() {
        Item item = new Item();
        item.setUri("uri");
        item.setPublisher(new PublisherDetails(Publisher.BBC.key()));
        item.setBroadcasts(Lists.newArrayList(simpleBroadcast));
        Award award = new Award();
        award.setDescription("description");
        award.setTitle("title");
        award.setYear(2009);
        award.setOutcome("won");
        item.setAwards(ImmutableSet.of(award));
        return item;
    }

    private Item getItemWithEventRefs(){
        Item item = new Item();
        item.setId("12345");
        item.setUri("uri");
        item.setPublisher(new PublisherDetails(Publisher.BBC.key()));

        EventRef eventRef = new EventRef();
        eventRef.setId("1234");
        eventRef.setPublisher(new PublisherDetails(Publisher.BBC.key()));
        item.setEventRefs(ImmutableSet.of(eventRef));
        return item;

    }

    private Broadcast getSimpleBroadcast() {
        Broadcast broadcast = new Broadcast();

        broadcast.setTransmissionTime(new Date());
        broadcast.setBroadcastDuration(10);
        broadcast.setBroadcastOn("broadcastOn");

        broadcast.setRestriction(simpleRestriction);

        return broadcast;
    }

    private Restriction getSimpleRestriction() {
        Restriction restriction = new Restriction();

        restriction.setMinimumAge(12);
        restriction.setRestricted(true);
        restriction.setRating("rating");
        restriction.setAuthority("authority");
        restriction.setMessage("message");

        return restriction;
    }

    private Location getSimpleLocationWithDuration(int duration) {
        Location location = new Location();
        location.setDuration(duration);
        return location;
    }
}
