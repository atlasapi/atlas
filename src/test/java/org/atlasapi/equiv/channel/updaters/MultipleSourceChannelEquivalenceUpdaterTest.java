package org.atlasapi.equiv.channel.updaters;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MultipleSourceChannelEquivalenceUpdaterTest {

    private MultipleSourceChannelEquivalenceUpdater multipleUpdater =
            MultipleSourceChannelEquivalenceUpdater.create();

    @Mock
    private EquivalenceUpdater<Channel> equivUpdater1 = mock(EquivalenceUpdater.class);
    @Mock
    private EquivalenceUpdater<Channel> equivUpdater2 = mock(EquivalenceUpdater.class);
    @Mock
    private EquivalenceUpdater<Channel> equivUpdater3 = mock(EquivalenceUpdater.class);
    @Mock
    private OwlTelescopeProxy telescopeProxy = mock(OwlTelescopeProxy.class);

    @Before
    public void setUp() {
        when(equivUpdater1.updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class))).thenReturn(true);
        when(equivUpdater2.updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class))).thenReturn(true);
        when(equivUpdater3.updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class))).thenReturn(true);
    }

    @Test
    public void retrievesCorrectUpdaterFromSource() throws Exception {
        multipleUpdater.register(Publisher.BT_TV_CHANNELS, equivUpdater1);
        multipleUpdater.register(Publisher.BT_TV_CHANNELS_TEST1, equivUpdater2);
        multipleUpdater.register(Publisher.BT_TV_CHANNELS_REFERENCE, equivUpdater3);

        multipleUpdater.updateEquivalences(channelForSource(Publisher.BT_TV_CHANNELS), telescopeProxy);
        multipleUpdater.updateEquivalences(channelForSource(Publisher.BT_TV_CHANNELS_REFERENCE), telescopeProxy);
        verify(equivUpdater1, atMost(1)).updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class));
        verify(equivUpdater2, never()).updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class));
        verify(equivUpdater3, atMost(1)).updateEquivalences(any(Channel.class), any(OwlTelescopeProxy.class));

    }

    private Channel channelForSource(Publisher publisher) {
        return Channel.builder()
                .withSource(publisher)
                .build();
    }

}
