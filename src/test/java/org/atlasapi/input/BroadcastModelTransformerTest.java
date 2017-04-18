package org.atlasapi.input;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.simple.BlackoutRestriction;
import org.atlasapi.media.entity.simple.Broadcast;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class BroadcastModelTransformerTest {

    private static final DateTime today = DateTime.parse("2017-04-11");

    private Broadcast simpleBroadcast;
    private BroadcastModelTransformer transformer;

    @Before
    public void setUp() {
        simpleBroadcast = createSimpleBroadcast();

        ChannelResolver channelResolver = Mockito.mock(ChannelResolver.class);
        transformer = BroadcastModelTransformer.create(channelResolver);
    }

    @Test
    public void testBroadcastGetsTransformedCorrectly() {
        org.atlasapi.media.entity.Broadcast complexBroadcast = transformer.transform(simpleBroadcast);

        assertThat(complexBroadcast.getBroadcastOn(), is("home"));
        assertThat(complexBroadcast.getTransmissionTime(), is(today));
        assertThat(complexBroadcast.getTransmissionEndTime(), is(today.plusDays(1)));
        assertThat(complexBroadcast.getScheduleDate(), is(today.toLocalDate()));
        assertThat(complexBroadcast.getRepeat(), is(false));
        assertThat(complexBroadcast.getSubtitled(), is(true));
        assertThat(complexBroadcast.getSigned(), is(false));
        assertThat(complexBroadcast.getAudioDescribed(), is(true));
        assertThat(complexBroadcast.getHighDefinition(), is(false));
        assertThat(complexBroadcast.getWidescreen(), is(true));
        assertThat(complexBroadcast.getSurround(), is(false));
        assertThat(complexBroadcast.getLive(), is(true));
        assertThat(complexBroadcast.getAliasUrls(), is(ImmutableSet.of("alias")));
        assertThat(complexBroadcast.getBlackoutRestriction(), is(notNullValue()));
        assertThat(complexBroadcast.getRevisedRepeat(), is(false));
        assertThat(complexBroadcast.getContinuation(), is(true));
        assertThat(complexBroadcast.getNewOneOff(), is(false));
    }

    private Broadcast createSimpleBroadcast() {
        Broadcast broadcast = new Broadcast("home", today, today.plusDays(1));

        broadcast.setScheduleDate(today.toLocalDate());
        broadcast.setRepeat(false);
        broadcast.setSubtitled(true);
        broadcast.setSigned(false);
        broadcast.setAudioDescribed(true);
        broadcast.setHighDefinition(false);
        broadcast.setWidescreen(true);
        broadcast.setSurround(false);
        broadcast.setLive(true);
        broadcast.setAliases(ImmutableSet.of("alias"));
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));
        broadcast.setRevisedRepeat(false);
        broadcast.setContinuation(true);
        broadcast.setNewOneOff(false);

        return broadcast;
    }
}
