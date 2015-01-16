package org.atlasapi.output.simple;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import junit.framework.TestCase;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class ChannelNumberingSimplificationTest extends TestCase {
    
    private static final DummyChannelGroupStore channelGroupStore = new DummyChannelGroupStore();
    private static final ChannelSimplifier channelSimplifier = new ChannelSimplifier(Mockito.mock(NumberToShortStringCodec.class), Mockito.mock(NumberToShortStringCodec.class), Mockito.mock(ChannelResolver.class), new PublisherSimplifier(), new ImageSimplifier(), 
            new ChannelGroupSummarySimplifier(Mockito.mock(NumberToShortStringCodec.class), channelGroupStore), channelGroupStore);
    private static final ChannelGroupSimplifier channelGroupSimplifier = new ChannelGroupSimplifier(Mockito.mock(NumberToShortStringCodec.class), channelGroupStore, new PublisherSimplifier());
    private static final ChannelNumberingChannelGroupModelSimplifier nestedChannelGroupSimplifier = new ChannelNumberingChannelGroupModelSimplifier(channelGroupSimplifier);
    private static final ChannelNumberingsChannelToChannelGroupModelSimplifier numberingSimplifier = new ChannelNumberingsChannelToChannelGroupModelSimplifier(channelGroupStore, nestedChannelGroupSimplifier);
    
    private static final ChannelModelSimplifier simplifier = new ChannelModelSimplifier(channelSimplifier, numberingSimplifier);
    
    private final Channel channel = Channel.builder()
        .withMediaType(MediaType.VIDEO)
        .withSource(Publisher.BBC)
        .build();
    
    private static final long channelId = 1234L;
    private static final long channelGroupId = 5678L;
    private static final long bbcPublisherChannelGroupId = 5679L;
    private static final ApplicationConfiguration appConfigWithMb = 
            ApplicationConfiguration.DEFAULT_CONFIGURATION
                                    .withSource(Publisher.BBC, SourceStatus.AVAILABLE_DISABLED)
                                    .withSource(Publisher.METABROADCAST, SourceStatus.AVAILABLE_ENABLED);
    
    private static final ApplicationConfiguration appConfigWithBbc = 
            ApplicationConfiguration.DEFAULT_CONFIGURATION
                                    .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                                    .withSource(Publisher.METABROADCAST, SourceStatus.AVAILABLE_DISABLED);
    
    private static final ApplicationConfiguration appConfigWithBbcAndMb = 
            ApplicationConfiguration.DEFAULT_CONFIGURATION
                                    .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                                    .withSource(Publisher.METABROADCAST, SourceStatus.AVAILABLE_ENABLED);
   
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        channelGroupStore.reset();
        channel.setId(channelId);
        
        Platform platform = new Platform();
        platform.setId(channelGroupId);
        platform.setPublisher(Publisher.METABROADCAST);
        
        channelGroupStore.createOrUpdate(platform);
        
        Platform bbcPublishedPlatform = new Platform();
        bbcPublishedPlatform.setId(bbcPublisherChannelGroupId);
        bbcPublishedPlatform.setPublisher(Publisher.BBC);
        
        channelGroupStore.createOrUpdate(bbcPublishedPlatform);
    }
    
    @Test
    public void testChannelNumberingAfterEndDate() {
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        
        LocalDate startDate = new LocalDate().minusYears(10);
        LocalDate endDate = new LocalDate().minusYears(9);
        ChannelNumbering numbering = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("2")
            .withStartDate(startDate)
            .withEndDate(endDate)
            .build();
        
        channel.addChannelNumber(numbering);
        channelGroup.addChannelNumbering(numbering);
        
        channelGroup = channelGroupStore.createOrUpdate(channelGroup);
        org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.HISTORY), appConfigWithBbc);
        org.atlasapi.media.entity.simple.ChannelNumbering simpleNumbering = Iterables.getOnlyElement(simpleChannel.getChannelGroups());
        assertEquals(startDate.toDate(), simpleNumbering.getStartDate());
        assertEquals(endDate.toDate(), simpleNumbering.getEndDate());
        
        simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS), appConfigWithBbc);
        assertTrue(simpleChannel.getChannelGroups().isEmpty());
    }

    @Test 
    public void testMultipleCurrentNumberings() {
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        
        ChannelNumbering one = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("1")
            .withStartDate(new LocalDate(2000, 1, 1))
            .build();
        
        ChannelNumbering two = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("2")
            .withStartDate(new LocalDate(2000, 1, 1))
            .build();
        
        channel.addChannelNumber(one);
        channel.addChannelNumber(two);
        channelGroup.addChannelNumbering(one);
        channelGroup.addChannelNumbering(two);
        
        channelGroup = channelGroupStore.createOrUpdate(channelGroup);
        org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS), appConfigWithBbc);
        assertThat(simpleChannel.getChannelGroups().size(), is(2));
        
        assertEquals("1", simpleChannel.getChannelGroups().get(0).getChannelNumber());
        assertEquals("2", simpleChannel.getChannelGroups().get(1).getChannelNumber());
    }
    
    @Test 
    public void testDefaultsToCurrentFuture() {
        
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        
        ChannelNumbering past = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("1")
            .withStartDate(new LocalDate().minusYears(10))
            .withEndDate(new LocalDate().minusYears(9))
            .build();
        
        ChannelNumbering current = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("2")
            .withStartDate(new LocalDate().minusYears(5))
            .withEndDate(new LocalDate().plusYears(2))
            .build();
        
        ChannelNumbering future = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("3")
            .withStartDate(new LocalDate().plusYears(1))
            .build();
        
        channel.addChannelNumber(past);
        channel.addChannelNumber(current);
        channel.addChannelNumber(future);
        channelGroup.addChannelNumbering(past);
        channelGroup.addChannelNumbering(current);
        channelGroup.addChannelNumbering(future);
        
        channelGroup = channelGroupStore.createOrUpdate(channelGroup);

        org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS), appConfigWithBbc);
        assertThat(simpleChannel.getChannelGroups().size(), is(2));
        
        org.atlasapi.media.entity.simple.ChannelNumbering first = simpleChannel.getChannelGroups().get(0);
        org.atlasapi.media.entity.simple.ChannelNumbering second = simpleChannel.getChannelGroups().get(1);
        
        assertThat(first.getChannelNumber(), org.hamcrest.Matchers.isOneOf("2", "3"));
        assertThat(second.getChannelNumber(), org.hamcrest.Matchers.isOneOf("2", "3"));
    }
    
    @Test
    public void testHistoryShowsAll() {
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        
        ChannelNumbering past = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("1")
            .withStartDate(new LocalDate().minusYears(10))
            .withEndDate(new LocalDate().minusYears(9))
            .build();
        
        ChannelNumbering current = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("2")
            .withStartDate(new LocalDate().minusYears(5))
            .build();
        
        ChannelNumbering future = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("3")
            .withStartDate(new LocalDate().plusYears(1))
            .build();
        
        channel.addChannelNumber(past);
        channel.addChannelNumber(current);
        channel.addChannelNumber(future);
        channelGroup.addChannelNumbering(past);
        channelGroup.addChannelNumbering(current);
        channelGroup.addChannelNumbering(future);
        
        channelGroup = channelGroupStore.createOrUpdate(channelGroup);
        
         org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.HISTORY), appConfigWithBbc);
        assertThat(simpleChannel.getChannelGroups().size(), is(1));
        
        org.atlasapi.media.entity.simple.ChannelNumbering first = Iterables.getOnlyElement(simpleChannel.getChannelGroups());
        assertThat(first.getHistory().size(), is(3));
    }
    
    @Test
    public void testHistoryWithMultipleCurrentChannels() {
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        
        ChannelNumbering past = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("1")
            .withStartDate(new LocalDate().minusYears(10))
            .withEndDate(new LocalDate().minusYears(9))
            .build();
        
        ChannelNumbering current = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("2")
            .withStartDate(new LocalDate().minusYears(5))
            .build();
        
        ChannelNumbering current2 = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("3")
            .withStartDate(new LocalDate().minusYears(5))
            .build();
        
        ChannelNumbering future = ChannelNumbering.builder()
            .withChannel(channelId)
            .withChannelGroup(channelGroupId)
            .withChannelNumber("4")
            .withStartDate(new LocalDate().plusYears(1))
            .build();
        
        channel.addChannelNumber(past);
        channel.addChannelNumber(current);
        channel.addChannelNumber(current2);
        channel.addChannelNumber(future);
        channelGroup.addChannelNumbering(past);
        channelGroup.addChannelNumbering(current);
        channelGroup.addChannelNumbering(current2);
        channelGroup.addChannelNumbering(future);
        
        channelGroup = channelGroupStore.createOrUpdate(channelGroup);
        
        org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.HISTORY), appConfigWithBbc);
        assertThat(simpleChannel.getChannelGroups().size(), is(2));
        
        org.atlasapi.media.entity.simple.ChannelNumbering first = Iterables.get(simpleChannel.getChannelGroups(), 0);
        org.atlasapi.media.entity.simple.ChannelNumbering second = Iterables.get(simpleChannel.getChannelGroups(), 1);
        assertThat(first.getHistory().size(), is(4));
        assertThat(second.getHistory().size(), is(4));
    }
    
    @Test
    public void testChannelGroupSummaryOutput() {
        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(channelGroupId).get();
        channelGroup.addChannel(channelId);
        Alias mbAlias = new Alias("my:namespace", "12345");
        channelGroup.addAlias(mbAlias);
        channelGroupStore.createOrUpdate(channelGroup);
        
        ChannelGroup bbcPublisherChannelGroup = channelGroupStore.channelGroupFor(bbcPublisherChannelGroupId).get();
        bbcPublisherChannelGroup.addChannel(channelId);
        Alias bbcAlias = new Alias("myother:namespace", "12347");
        bbcPublisherChannelGroup.addAlias(bbcAlias);
        channelGroupStore.createOrUpdate(bbcPublisherChannelGroup);
        
        // 
        // Test with key that has MB enabled only
        //
        
        org.atlasapi.media.entity.simple.Channel simpleChannel = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.CHANNEL_GROUPS_SUMMARY), appConfigWithMb);
        org.atlasapi.media.entity.simple.Alias alias = Iterables.getOnlyElement(Iterables.getOnlyElement(simpleChannel.getGroups()).getAliases());
        
        assertEquals(mbAlias.getNamespace(), alias.getNamespace());
        assertEquals(mbAlias.getValue(), alias.getValue());
        
        // 
        // Test with key that has BBC enabled only
        //
        
        org.atlasapi.media.entity.simple.Channel simpleChannelWithBbc = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.CHANNEL_GROUPS_SUMMARY), appConfigWithBbc);
        org.atlasapi.media.entity.simple.Alias simpleActualBbcAlias = Iterables.getOnlyElement(Iterables.getOnlyElement(simpleChannelWithBbc.getGroups()).getAliases());
        
        assertEquals(bbcAlias.getNamespace(), simpleActualBbcAlias.getNamespace());
        assertEquals(bbcAlias.getValue(), simpleActualBbcAlias.getValue());
        
        //
        // Test with key that has BBC + MB enabled
        //
        
        org.atlasapi.media.entity.simple.Channel simpleWithBoth = simplifier.simplify(channel, ImmutableSet.of(Annotation.CHANNEL_GROUPS, Annotation.CHANNEL_GROUPS_SUMMARY), appConfigWithBbcAndMb);
        assertEquals(simpleWithBoth.getGroups().size(), 2);
        
    }
    
    private static class DummyChannelGroupStore implements ChannelGroupStore {

        private Map<Long, ChannelGroup> channelGroups;
        private Multimap<Long, ChannelGroup> channelGroupsForChannel;
        
        public DummyChannelGroupStore() {
            reset();
        }
        
        public void reset() {
            channelGroups = Maps.newHashMap();
            channelGroupsForChannel = HashMultimap.create();
        }
        
        @Override
        public Optional<ChannelGroup> fromAlias(String alias) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<ChannelGroup> channelGroupFor(Long id) {
            return Optional.fromNullable(channelGroups.get(id));
        }

        @Override
        public Iterable<ChannelGroup> channelGroupsFor(Iterable<? extends Long> ids) {
            return Iterables.transform(channelGroups.keySet(), new Function<Long, ChannelGroup>() {
                @Override
                public ChannelGroup apply(Long input) {
                    return channelGroups.get(input);
                }
            });
        }

        @Override
        public Iterable<ChannelGroup> channelGroups() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<ChannelGroup> channelGroupsFor(Channel channel) {
            return channelGroupsForChannel.get(channel.getId());
        }

        @Override
        public ChannelGroup createOrUpdate(ChannelGroup channelGroup) {
            channelGroups.put(channelGroup.getId(), channelGroup);
            for (Long channel : channelGroup.getChannels()) {
                channelGroupsForChannel.put(channel, channelGroup);
            }
            return channelGroup;
        }

        @Override
        public Optional<ChannelGroup> channelGroupFor(String canonicalUri) {
            throw new UnsupportedOperationException();
        }
        
    }
}
