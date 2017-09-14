package org.atlasapi.remotesite.youview;

import java.util.Set;

import com.google.common.collect.ImmutableMultimap;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DefaultYouViewChannelResolverTest {

    private static final Set<String> ALIAS_PREFIXES =
            ImmutableSet.of("http://youview.com/service/");
    
    public static final Channel BBC_ONE = new Channel(
            Publisher.METABROADCAST,
            "BBC One",
            "bbcone",
            null,
            MediaType.VIDEO,
            "http://www.bbc.co.uk/bbcone"
    );
    public static final Channel BBC_TWO = new Channel(
            Publisher.METABROADCAST,
            "BBC Two",
            "bbctwo",
            null,
            MediaType.VIDEO,
            "http://www.bbc.co.uk/bbctwo"
    );

    private ChannelResolver channelResolver;

    @Before
    public void setUp() {
        channelResolver = mock(ChannelResolver.class);
        when(channelResolver.allForAliases(anyString())).thenReturn(ImmutableMultimap.of());
    }
    
    @Test
    public void testResolvesByServiceId() {
        when(channelResolver.allForAliases("http://youview.com/service/"))
            .thenReturn(ImmutableMultimap.of("http://youview.com/service/123", BBC_ONE));
        
        DefaultYouViewChannelResolver yvChannelResolver =
                DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);
        
        assertThat(yvChannelResolver.getChannels(123).iterator().next(), is(BBC_ONE));
    }
    
    @Test
    public void testOverrides() {
        when(channelResolver.allForAliases("http://youview.com/service/"))
            .thenReturn(ImmutableMultimap.of("http://youview.com/service/123", BBC_ONE));
        when(channelResolver.allForAliases("http://overrides.youview.com/service/"))
            .thenReturn(ImmutableMultimap.of("http://overrides.youview.com/service/456", BBC_ONE));
    
        
        DefaultYouViewChannelResolver yvChannelResolver =
                DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);
        
        assertThat(yvChannelResolver.getChannels(456).iterator().next(), is(BBC_ONE));
        assertThat("Shouldn't be able to look up by overridden service ID",
                    yvChannelResolver.getChannels(123).isEmpty(), is(true));
        
        assertThat(yvChannelResolver.getServiceAliases(BBC_ONE),
                is(ImmutableSet.of("http://youview.com/service/456")));
    }
    
    @Test
    public void testOverridesWithoutPrimaryId() {
        when(channelResolver.allForAliases("http://overrides.youview.com/service/"))
            .thenReturn(ImmutableMultimap.of("http://overrides.youview.com/service/456", BBC_ONE));

        DefaultYouViewChannelResolver yvChannelResolver =
                DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);
    
        assertThat(yvChannelResolver.getChannels(456).iterator().next(), is(BBC_ONE));
    }

    @Test
    public void overrideAliasTakesPrecedence() {
        when(channelResolver.allForAliases("http://youview.com/service/")).thenReturn(
                ImmutableMultimap.of("http://youview.com/service/123", BBC_ONE));
        when(channelResolver.allForAliases("http://overrides.youview.com/service/")).thenReturn(
                ImmutableMultimap.of("http://overrides.youview.com/service/123", BBC_TWO));


        DefaultYouViewChannelResolver yvChannelResolver =
                DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);

        assertThat(yvChannelResolver.getChannels(123).iterator().next(), is(BBC_TWO));
    }

    @Test
    public void gettingMultipleChannelsOnlyResolvesOnceFromStore() {
        when(channelResolver.allForAliases("http://youview.com/service/"))
                .thenReturn(ImmutableMultimap.of(
                        "http://youview.com/service/123", BBC_ONE,
                        "http://youview.com/service/456", BBC_TWO
                ));

        DefaultYouViewChannelResolver yvChannelResolver =
                DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);

        yvChannelResolver.getChannels(123);
        yvChannelResolver.getAllChannels();
        yvChannelResolver.getChannels(456);
        yvChannelResolver.getAllChannels();

        // We access the resolver twice on every single resolution. Once for regular
        // and once for override channels
        verify(channelResolver, times(2)).allForAliases(anyString());
    }
}
