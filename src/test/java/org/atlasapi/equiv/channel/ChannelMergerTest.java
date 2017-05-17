package org.atlasapi.equiv.channel;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mock;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChannelMergerTest {

    private final ChannelMerger channelMerger;

    @Mock
    private Application btProdApp = mock(Application.class);

    @Mock
    private Application btTest1App = mock(Application.class);

    @Mock
    private Application someOtherApp = mock(Application.class);


    public ChannelMergerTest() {
        this.channelMerger = ChannelMerger.create();

        when(btProdApp.getConfiguration()).thenReturn(
                ApplicationConfiguration.builder()
                    .withPrecedence(ImmutableList.of(
                            Publisher.BT_TVE_VOD,
                            Publisher.BT_TV_CHANNELS,
                            Publisher.BT_TV_CHANNELS_TEST1,
                            Publisher.PA,
                            Publisher.METABROADCAST
                    ))
                    .withEnabledWriteSources(ImmutableList.of())
                    .build()
        );

        when(btTest1App.getConfiguration()).thenReturn(
                ApplicationConfiguration.builder()
                    .withPrecedence(ImmutableList.of(
                            Publisher.BT_TVE_VOD,
                            Publisher.BT_TV_CHANNELS_TEST1,
                            Publisher.PA,
                            Publisher.METABROADCAST
                    ))
                    .withEnabledWriteSources(ImmutableList.of())
                    .build()
        );

        when(someOtherApp.getConfiguration()).thenReturn(
                ApplicationConfiguration.builder()
                        .withNoPrecedence(ImmutableList.of(
                                Publisher.BT_TVE_VOD,
                                Publisher.PA,
                                Publisher.METABROADCAST,
                                Publisher.BT_TV_CHANNELS_TEST1
                        ))
                        .withEnabledWriteSources(ImmutableList.of())
                        .build()
        );

    }

    @Test
    public void testMergesWithBtProdData() throws Exception {

        Channel channel = channelMerger.mergeChannel(btProdApp, channelMerger.getChannel(123L));

        assertThat(channel.getId(), is(channelMerger.getChannel(123L).getId()));
        assertThat(channel.getAdvertiseFrom(), is(channelMerger.getChannel(111L).getAdvertiseFrom()));
        assertThat(channel.getAdvertiseTo(), is(channelMerger.getChannel(111L).getAdvertiseTo()));
        assertThat(channel.getSource(), is(channelMerger.getChannel(123L).getSource()));

    }

    @Test
    public void testMergesWithBtVolEDataAndUsesExistingAsFallback() throws Exception {

        Channel channel = channelMerger.mergeChannel(btTest1App, channelMerger.getChannel(123L));

        assertThat(channel.getId(), is(channelMerger.getChannel(123L).getId()));
        assertThat(channel.getAdvertiseFrom(), is(channelMerger.getChannel(888L).getAdvertiseFrom()));
        assertThat(channel.getAdvertiseTo(), is(channelMerger.getChannel(123L).getAdvertiseTo()));
        assertThat(channel.getSource(), is(channelMerger.getChannel(123L).getSource()));

    }

    @Test
    public void testDoesntMergeOnApplicationWithNoPrecedence() throws Exception {

        Channel channel = channelMerger.mergeChannel(someOtherApp, channelMerger.getChannel(123L));

        assertThat(channel.getId(), is(channelMerger.getChannel((123L)).getId()));
        assertThat(channel.getAdvertiseFrom(), is(channelMerger.getChannel(123L).getAdvertiseFrom()));
        assertThat(channel.getAdvertiseTo(), is(channelMerger.getChannel(123L).getAdvertiseTo()));
        assertThat(channel.getSource(), is(channelMerger.getChannel(123L).getSource()));

    }

    @Test
    public void testDatesAreNullWhenNoPrecedenceOnApplication() throws Exception {

        Channel channel = channelMerger.mergeChannel(someOtherApp, channelMerger.getChannel(456L));

        assertThat(channel.getId(), is(channelMerger.getChannel((456L)).getId()));
        assertThat(channel.getAdvertiseFrom(), is(Matchers.nullValue()));
        assertThat(channel.getAdvertiseTo(), is(Matchers.nullValue()));
        assertThat(channel.getSource(), is(channelMerger.getChannel(456L).getSource()));
    }

    @Test
    public void testDoesntMergeWhenRequestingEquivalentChannel() throws Exception {

        Channel channel = channelMerger.mergeChannel(btProdApp, channelMerger.getChannel(111L));

        assertThat(channel.getId(), is(channelMerger.getChannel((111L)).getId()));
        assertThat(channel.getAdvertiseFrom(), is(channelMerger.getChannel(111L).getAdvertiseFrom()));
        assertThat(channel.getAdvertiseTo(), is(channelMerger.getChannel(111L).getAdvertiseTo()));
        assertThat(channel.getSource(), is(channelMerger.getChannel(111L).getSource()));
    }

}
