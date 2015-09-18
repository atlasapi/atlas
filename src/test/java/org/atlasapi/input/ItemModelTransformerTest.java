package org.atlasapi.input;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Date;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Broadcast;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.media.entity.simple.Restriction;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

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
        org.atlasapi.media.entity.Item complex = transformer.transform(simpleItem);

        assertThat(complex.getVersions().size(), is(1));

        Version version = complex.getVersions().iterator().next();
        checkRestriction(version.getRestriction());
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
}