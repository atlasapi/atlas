package org.atlasapi.remotesite.youview;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class DefaultYouViewChannelResolverTest {

    private static final Set<String> ALIAS_PREFIXES = ImmutableSet.of("http://youview.com/service/");
    
    private static final Channel BBC_ONE = new Channel(Publisher.METABROADCAST, "BBC One", "bbcone", null, MediaType.VIDEO, "http://www.bbc.co.uk/bbcone");
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    
    @Test
    public void testResolvesByServiceId() {
        when(channelResolver.forAliases("http://youview.com/service/"))
            .thenReturn(ImmutableMap.of("http://youview.com/service/123", BBC_ONE));
        
        DefaultYouViewChannelResolver yvChannelResolver = new DefaultYouViewChannelResolver(channelResolver, ALIAS_PREFIXES);
        
        assertThat(yvChannelResolver.getChannel(123).get(), is(BBC_ONE));
    }
    
    @Test
    public void testOverrides() {
        when(channelResolver.forAliases("http://youview.com/service/"))
            .thenReturn(ImmutableMap.of("http://youview.com/service/123", BBC_ONE));
        when(channelResolver.forAliases("http://overrides.youview.com/service/"))
            .thenReturn(ImmutableMap.of("http://overrides.youview.com/service/456", BBC_ONE));
    
        
        DefaultYouViewChannelResolver yvChannelResolver = new DefaultYouViewChannelResolver(channelResolver, ALIAS_PREFIXES);
        
        assertThat(yvChannelResolver.getChannel(456).get(), is(BBC_ONE));
        assertFalse("Shouldn't be able to look up by overridden service ID", 
                    yvChannelResolver.getChannel(123).isPresent());
        
        assertThat(yvChannelResolver.getChannelServiceAlias(456), 
                is("http://youview.com/service/456"));
    }
    
    @Test
    public void testOverridesWithoutPrimaryId() {
        when(channelResolver.forAliases("http://overrides.youview.com/service/"))
            .thenReturn(ImmutableMap.of("http://overrides.youview.com/service/456", BBC_ONE));

        DefaultYouViewChannelResolver yvChannelResolver = new DefaultYouViewChannelResolver(channelResolver, ALIAS_PREFIXES);
    
        assertThat(yvChannelResolver.getChannel(456).get(), is(BBC_ONE));
    }
}
