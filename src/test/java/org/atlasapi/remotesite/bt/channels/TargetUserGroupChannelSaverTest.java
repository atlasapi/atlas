package org.atlasapi.remotesite.bt.channels;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;


@RunWith( MockitoJUnitRunner.class )
public class TargetUserGroupChannelSaverTest {
    
    private static final long TARGET_USER_GROUP_A_GROUP_ID = 1;
    private static final long TARGET_USER_GROUP_B_GROUP_ID = 2;
    private static final Channel CHANNEL1 = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");
    private static final Channel CHANNEL2 = new Channel(Publisher.METABROADCAST, "Channel 2", "b", true, MediaType.VIDEO, "http://channel2.com");
    
    private static final String TARGET_USER_GROUP_B = "gbr-B";
    private static final String TARGET_USER_GROUP_A = "gbr-A";
    private static final String CHANNEL1_KEY = "kdcv";
    private static final String CHANNEL2_KEY = "hqcs";
     
    private static final Map<String, String> targetUserGroupKeyToUri = 
            ImmutableMap.of(
                    TARGET_USER_GROUP_A, "http://example.org/a", 
                    TARGET_USER_GROUP_B, "http://example.org/b");
    
    private static final String ALIAS_URI_PREFIX = "http://example.org/";
    private static final String ALIAS_NAMESPACE = "a:namespace";
    
    private final ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
    private final ChannelGroupWriter channelGroupWriter = mock(ChannelGroupWriter.class);
    private final BtMpxClient btMpxClient = mock(BtMpxClient.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ChannelWriter channelWriter = mock(ChannelWriter.class);
    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    
    private final Long channel1Id = codec.decode(CHANNEL1_KEY).longValue();
    private final Long channel2Id = codec.decode(CHANNEL2_KEY).longValue();
    
    private final TargetUserGroupChannelGroupSaver saver 
        = new TargetUserGroupChannelGroupSaver(Publisher.METABROADCAST, ALIAS_URI_PREFIX, ALIAS_NAMESPACE, 
                channelGroupResolver, channelGroupWriter, btMpxClient, channelResolver, channelWriter);
    
    @Before
    public void setUp() {
        CHANNEL1.setId(channel1Id);
        CHANNEL2.setId(channel2Id);
    }
    
    @Test
    public void testExtractsTargetUserGroups() throws BtMpxClientException {
        
        when(channelGroupResolver.channelGroupFor(canonicalUriFor(TARGET_USER_GROUP_A)))
            .thenReturn(Optional.<ChannelGroup>of(channelGroup(TARGET_USER_GROUP_A, TARGET_USER_GROUP_A_GROUP_ID)));
        when(channelGroupResolver.channelGroupFor(canonicalUriFor(TARGET_USER_GROUP_B)))
            .thenReturn(Optional.<ChannelGroup>of(channelGroup(TARGET_USER_GROUP_B, TARGET_USER_GROUP_B_GROUP_ID)));
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel1Id)))
            .thenReturn(ImmutableSet.of(CHANNEL1));
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel2Id)))
            .thenReturn(ImmutableSet.of(CHANNEL2));
        when(btMpxClient.getCategories(Optional.<Selection>absent()))
            .thenReturn(categoryLookups());
        saver.update(ImmutableList.of(channelWithTargetUserGroup(CHANNEL1_KEY, TARGET_USER_GROUP_A),
                channelWithTargetUserGroup(CHANNEL2_KEY, TARGET_USER_GROUP_B)));
        
        ArgumentCaptor<Channel> channelCaptor = ArgumentCaptor.forClass(Channel.class);
        verify(channelWriter, times(2)).createOrUpdate(channelCaptor.capture());

        Map<Long, Channel> map = Maps.uniqueIndex(channelCaptor.getAllValues(), new Function<Channel, Long>() {

            @Override
            public Long apply(Channel input) {
                return input.getId();
            }
            
        });
        
        assertThat(Iterables.getOnlyElement(map.get(channel1Id).getChannelNumbers()).getChannelGroup(), 
                is(TARGET_USER_GROUP_A_GROUP_ID));
        assertThat(Iterables.getOnlyElement(map.get(channel2Id).getChannelNumbers()).getChannelGroup(), 
                is(TARGET_USER_GROUP_B_GROUP_ID));
    }
    
    private PaginatedEntries categoryLookups() {
        Builder<Entry> entries = ImmutableList.builder();
        for (Map.Entry<String, String> entry : targetUserGroupKeyToUri.entrySet()) {
            entries.add(new Entry(entry.getValue(), 0, entry.getKey(), ImmutableList.<Category>of(),
                    ImmutableList.<Content>of(), true, "title", "targetUserGroup", true, false, 0, "urn:BT:linear:service:751764"));
        }
        return new PaginatedEntries(0, 100, 2, "", entries.build());
    }

    public Entry channelWithTargetUserGroup(String channelId, String name) {
        //{"name":"S0128048","scheme":"subscription","label":""},{"name":"Linear TUG - GBR-bt_multicast","scheme":"targetUserGroup","label":"GBR-bt_multicast"},{"name":"Linear TUG - GBR-bt_broadband","scheme":"targetUserGroup","label":"GBR-bt_broadband"}
        Category category = new Category(name, "targetUserGroup", "Linear TUG");
        return new Entry(channelId, 0, "Title", 
                    ImmutableList.of(category), 
                    ImmutableList.<Content>of(), 
                    true, null, null, true, false, 0, "urn:BT:linear:service:751764");
    }
    
    private String canonicalUriFor(String key) {
        return ALIAS_URI_PREFIX + targetUserGroupKeyToUri.get(key).replace("http://", "");
    }
    
    private ChannelGroup channelGroup(String key, long atlasId) {
        ChannelGroup group = new Region();
        group.setCanonicalUri(canonicalUriFor(key));
        group.setId(atlasId);
        group.setAliases(ImmutableSet.of(new Alias(ALIAS_NAMESPACE, key)));
        return group;
    }
}
