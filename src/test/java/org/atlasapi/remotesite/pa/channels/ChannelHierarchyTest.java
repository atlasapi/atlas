package org.atlasapi.remotesite.pa.channels;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.pa.channels.bindings.Channels;
import org.atlasapi.remotesite.pa.channels.bindings.Logo;
import org.atlasapi.remotesite.pa.channels.bindings.Logos;
import org.atlasapi.remotesite.pa.channels.bindings.Name;
import org.atlasapi.remotesite.pa.channels.bindings.Names;
import org.atlasapi.remotesite.pa.channels.bindings.ProviderChannelId;
import org.atlasapi.remotesite.pa.channels.bindings.ProviderChannelIds;
import org.atlasapi.remotesite.pa.channels.bindings.Variation;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ChannelHierarchyTest {

    private final PaChannelsIngester ingester = new PaChannelsIngester();
    
    @Test
    public void testStationWithSingleChannel() {
        ChannelInfo channel = new ChannelInfo();
        channel.setStartDate("2009-06-06");
        channel.setId("1741");
        channel.setName("Heat TV", "2009-10-11", "2010-01-10");
        channel.setName("Newer Heat TV", "2010-01-11", null);
        channel.setImage("p131906.png", "2009-10-11", 360, 240);
        channel.addProviderAlias("9", "1078");
        channel.setFormat();
        channel.setRegional();
        channel.setTimeshift("60");
        
        StationInfo station = new StationInfo();
        station.setId("1");
        station.setName("Heat TV", "2002-03-12");
        // TODO test images on stations
//        station.setImage("", "");
        station.addChannel(channel);
        
        ServiceProviderInfo serviceProvider = new ServiceProviderInfo();
        serviceProvider.setId("9");
        serviceProvider.setName("YouView", "2000-01-01");
        
        ChannelTree tree = ingester.processStation(station.createPaStation(), ImmutableList.of(serviceProvider.build()));
        Channel createdChannel = Iterables.getOnlyElement(tree.getChildren());
        
        assertEquals("http://ref.atlasapi.org/channels/pressassociation.com/1741", createdChannel.getCanonicalUri());
        
        String firstAlias = Iterables.get(createdChannel.getAliasUrls(), 0);
        String secondAlias = Iterables.get(createdChannel.getAliasUrls(), 1);
        assertThat(firstAlias, isOneOf("http://pressassociation.com/channels/1741", "http://youview.com/service/1078"));
        assertThat(secondAlias, isOneOf("http://pressassociation.com/channels/1741", "http://youview.com/service/1078"));
        
        assertEquals("Newer Heat TV", createdChannel.getTitle());
        assertEquals(new LocalDate(2009, 6, 6), createdChannel.getStartDate());
        
        TemporalField<String> oldName = new TemporalField<String>("Heat TV", new LocalDate(2009, 10, 11), new LocalDate(2010, 1, 11));
        TemporalField<String> newName = new TemporalField<String>("Newer Heat TV", new LocalDate(2010, 1, 11), null);
        assertEquals(ImmutableSet.of(oldName, newName), ImmutableSet.copyOf(createdChannel.getAllTitles()));
        
        Image expectedImage = new Image("http://images.atlas.metabroadcast.com/pressassociation.com/channels/p131906.png");
        expectedImage.setType(ImageType.LOGO);
        expectedImage.setColor(ImageColor.COLOR);
        expectedImage.setWidth(360);
        expectedImage.setHeight(240);
        assertEquals(ImmutableSet.of(expectedImage), createdChannel.getImages());
        assertEquals(new TemporalField<Image>(expectedImage, new LocalDate(2009, 10, 11), null), Iterables.getOnlyElement(createdChannel.getAllImages()));
        
        assertEquals(Publisher.METABROADCAST, createdChannel.getSource());
        assertTrue(createdChannel.getHighDefinition());
        assertTrue(createdChannel.getRegional());
        assertEquals(Duration.standardSeconds(3600), createdChannel.getTimeshift());
        assertTrue(createdChannel.isTimeshifted());
    }
    
    @Test
    public void testChannelWithMultipleYouViewServices() {
        ChannelInfo channel = new ChannelInfo();
        channel.setStartDate("2009-06-06");
        channel.setId("1741");
        channel.setName("Heat TV", "2009-10-11", "2010-01-10");
        channel.setName("Newer Heat TV", "2010-01-11", null);
        channel.setImage("p131906.png", "2009-10-11", 360, 240);
        channel.addProviderAlias("9", "1078");
        channel.addProviderAlias("10", "1079");
        channel.setFormat();
        channel.setRegional();
        channel.setTimeshift("60");
        
        StationInfo station = new StationInfo();
        station.setId("1");
        station.setName("Heat TV", "2002-03-12");
        station.addChannel(channel);
        
        ServiceProviderInfo serviceProviderYv = new ServiceProviderInfo();
        serviceProviderYv.setId("9");
        serviceProviderYv.setName("YouView", "2000-01-01");
        
        ServiceProviderInfo serviceProviderBt = new ServiceProviderInfo();
        serviceProviderBt.setId("10");
        serviceProviderBt.setName("BT TV", "2000-01-01");
        
        ChannelTree tree = ingester.processStation(station.createPaStation(), 
                ImmutableList.of(serviceProviderBt.build(),
                                 serviceProviderYv.build()));
        
        Channel createdChannel = Iterables.getOnlyElement(tree.getChildren());
        
        assertEquals(ImmutableSet.of("http://pressassociation.com/channels/1741", 
                                     "http://youview.com/service/1078", 
                                     "http://bt.youview.com/service/1079"), 
                     createdChannel.getAliasUrls());
    }
    
    @Test
    public void testStationWithMultipleChannels() {
        ChannelInfo westMidlands = new ChannelInfo();
        westMidlands.setStartDate("2010-06-01");
        westMidlands.setId("11");
        westMidlands.setName("BBC One West Midlands", "2011-09-28", null);
        westMidlands.setImage("p131474.png", "2011-09-28", 360, 240);
        westMidlands.setFormat();
        westMidlands.setMediaType("TV");
        
        ChannelInfo channelIslands = new ChannelInfo();
        channelIslands.setStartDate("2010-04-23");
        channelIslands.setEndDate("2013-04-23");
        channelIslands.setId("1663");
        channelIslands.setName("BBC One Channel Islands", "2011-10-15", null);
        channelIslands.setImage("p131731.png", "2011-10-15", 360, 240);
        channelIslands.setFormat();
        channelIslands.setMediaType("TV");
        
        StationInfo station = new StationInfo();
        station.setId("1");
        station.setName("BBC One", "2002-03-12");
        // TODO test images on stations
//        station.setImage("", "");
        station.addChannel(westMidlands);
        station.addChannel(channelIslands);
        
        ServiceProviderInfo serviceProvider = new ServiceProviderInfo();
        serviceProvider.setId("9");
        serviceProvider.setName("YouView", "2000-01-01");
        
        ChannelTree tree = ingester.processStation(station.createPaStation(), ImmutableList.of(serviceProvider.build()));
        
        Channel parent = tree.getParent();
        List<Channel> children = tree.getChildren();
        
        assertEquals(Publisher.METABROADCAST, parent.getSource());
        assertEquals("http://ref.atlasapi.org/channels/pressassociation.com/stations/1", parent.getUri());
        assertEquals("BBC One", parent.getTitle());
        assertEquals(new TemporalField<String>("BBC One", new LocalDate(2002, 3, 12), null), Iterables.getOnlyElement(parent.getAllTitles()));
//      assertEquals("", parent.image());
        
        assertEquals("http://pressassociation.com/stations/1", Iterables.getOnlyElement(parent.getAliasUrls()));
        
        assertTrue(parent.getHighDefinition());
        assertEquals(MediaType.VIDEO, parent.getMediaType());
        
        Channel westMids = Channel.builder()
            .withSource(Publisher.METABROADCAST)
            .withUri("http://ref.atlasapi.org/channels/pressassociation.com/11")
            .withTitle("BBC One West Midlands", new LocalDate(2011, 9, 28))
            .withImage(new Image("http://images.atlas.metabroadcast.com/pressassociation.com/channels/p131474.png"), new LocalDate(2011, 9, 28))
            .withStartDate(new LocalDate(2010, 6, 1))
            .withMediaType(MediaType.VIDEO)
            .withHighDefinition(true)
            .withRegional(false)
            .build();
        westMids.addAliasUrl("http://pressassociation.com/channels/11");
        
        Channel channelIsl = Channel.builder()
            .withSource(Publisher.METABROADCAST)
            .withUri("http://ref.atlasapi.org/channels/pressassociation.com/1663")
            .withTitle("BBC One Channel Islands", new LocalDate(2011, 10, 15))
            .withImage(new Image("http://images.atlas.metabroadcast.com/pressassociation.com/channels/p131731.png"), new LocalDate(2011, 10, 15))
            .withStartDate(new LocalDate(2010, 4, 23))
            .withMediaType(MediaType.VIDEO)
            .withHighDefinition(true)
            .withRegional(false)
            .build();
        channelIsl.addAliasUrl("http://pressassociation.com/channels/1663");
        
        ExtendedChannelEquivalence equiv = new ExtendedChannelEquivalence();
        assertTrue(equiv.pairwise().equivalent(ImmutableList.of(westMids, channelIsl), ImmutableList.copyOf(children)));
    }
    
    private class ExtendedChannelEquivalence extends Equivalence<Channel> {

        @Override
        protected boolean doEquivalent(Channel a, Channel b) {
            return a.getSource().equals(b.getSource())
                && Objects.equal(a.getUri(), b.getUri())
                && Objects.equal(a.getTitle(), b.getTitle())
                && Objects.equal(a.getImages(), b.getImages())
                && Objects.equal(a.getMediaType(), b.getMediaType())
                && Objects.equal(a.getHighDefinition(), b.getHighDefinition())
                && Objects.equal(a.getRegional(), b.getRegional())
                && Objects.equal(a.getTimeshift(), b.getTimeshift())
                && Objects.equal(a.isTimeshifted(), b.isTimeshifted())
                && Objects.equal(a.getVariations(), b.getVariations())
                && Objects.equal(a.getParent(), b.getParent())
                && Objects.equal(a.getStartDate(), b.getStartDate());
        }

        @Override
        protected int doHash(Channel t) {
            return 0;
        }
        
    }
    
    private class ChannelInfo {
        private Set<TimeboxedString> names = Sets.newHashSet();
        private String id;
        private String image;
        private String imageStartDate;
        private String imageWidth;
        private String imageHeight;
        private String startDate;
        private String endDate;
        private Set<ProviderChannelId> providerChannelIds = Sets.newHashSet();
        private String format;
        private boolean regional = false;
        private String timeshift;
        private String mediaType;
        
        public void setName(String name, String nameStartDate, String endDate) {
            TimeboxedString title = new TimeboxedString();
            title.setValue(name);
            title.setStartDate(nameStartDate);
            title.setEndDate(endDate);
            names.add(title);
        }
        public void setId(String id) {
            this.id = id;
        }
        public void setImage(String image, String imageStartDate, int width, int height) {
            this.image = image;
            this.imageStartDate = imageStartDate;
            this.imageWidth = "" + width;
            this.imageHeight = "" + height;
        }
        
        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }
        
        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }
        
        public void addProviderAlias(String serviceProviderId, String providerChannelId) {
            ProviderChannelId channelId = new ProviderChannelId();
            channelId.setServiceProviderId(serviceProviderId);
            channelId.setvalue(providerChannelId);
            providerChannelIds.add(channelId);
        }
        
        public void setFormat() {
            this.format = "HD";
        }
        
        public void setTimeshift(String timeshift) {
            this.timeshift = timeshift;
        }
        
        public void setRegional() {
            this.regional = true;
        }
        
        public void setMediaType(String mediaType) {
            this.mediaType = mediaType;
        }
        
        public org.atlasapi.remotesite.pa.channels.bindings.Channel createPaChannel() {
            org.atlasapi.remotesite.pa.channels.bindings.Channel paChannel = new org.atlasapi.remotesite.pa.channels.bindings.Channel();
            
            Names paChannelNames = new Names();
            for (TimeboxedString name : names) {
                Name paChannelName = new Name();
                paChannelName.setvalue(name.getValue());
                if (name.getStartDate() != null) {
                    paChannelName.setStartDate(name.getStartDate());
                }
                if (name.getEndDate() != null) {
                    paChannelName.setEndDate(name.getEndDate());
                }
                paChannelNames.getName().add(paChannelName);                
            }
            paChannel.setNames(paChannelNames);
            
            if (!providerChannelIds.isEmpty()) {
                ProviderChannelIds channelIds = new ProviderChannelIds();
                ProviderChannelId channelId = new ProviderChannelId();
                channelIds.getProviderChannelId().addAll(providerChannelIds);
                paChannel.setProviderChannelIds(channelIds);
            }
            
            Logos logos = new Logos();
            Logo logo = new Logo();
            logo.setvalue(image);
            logo.setStartDate(imageStartDate);
            logo.setHeight(imageHeight);
            logo.setWidth(imageWidth);
            logos.getLogo().add(logo);
            paChannel.setLogos(logos);
            
            paChannel.setId(id);
            paChannel.setStartDate(startDate);
            // TODO add this back in, as PaChannels will be able to have endDates
            //paChannel.setEndDate(endDate);
            if (format != null) {
                paChannel.setFormat(format);
            }
            
            if (timeshift != null) {
                Variation variation = new Variation();
                variation.setTimeshift(timeshift);
                variation.setType("HD");
                paChannel.getVariation().add(variation);
            }
            
            if (regional) {
                Variation variation = new Variation();
                variation.setType("regional");
                paChannel.getVariation().add(variation);
            }
            
            if (mediaType != null) {
                paChannel.setMediaType(mediaType);
            }
            
            return paChannel;
        }
    }
    
    private class TimeboxedString {
        private String value;
        private String startDate;
        private String endDate;
        
        public String getValue() {
            return value;
        }
        public void setValue(String value) {
            this.value = value;
        }
        public String getStartDate() {
            return startDate;
        }
        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }
        public String getEndDate() {
            return endDate;
        }
        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }
    }
    
    private class StationInfo {
        private String name;
        private String nameStartDate;
        private String id;
        private String image;
        private String imageStartDate;
        private List<ChannelInfo> channels = Lists.newArrayList();
        
        public void setName(String name, String nameStartDate) {
            this.name = name;
            this.nameStartDate = nameStartDate;
        }
        public void setId(String id) {
            this.id = id;
        }
        public void setImage(String image, String imageStartDate) {
            this.image = image;
            this.imageStartDate = imageStartDate;
        }
        
        public void addChannel(ChannelInfo channel) {
            channels.add(channel);
        }
        
        public org.atlasapi.remotesite.pa.channels.bindings.Station createPaStation() {
            org.atlasapi.remotesite.pa.channels.bindings.Station paStation = new org.atlasapi.remotesite.pa.channels.bindings.Station();
            
            Names paChannelNames = new Names();
            Name paChannelName = new Name();
            paChannelName.setvalue(name);
            paChannelName.setStartDate(nameStartDate);
            paChannelNames.getName().add(paChannelName);
            paStation.setNames(paChannelNames);
            
            // can't set station image currently
//            Logos logos = new Logos();
//            Logo logo = new Logo();
//            logo.setvalue(image);
//            logo.setStartDate(imageStartDate);
//            logos.getLogo().add(logo);
//            paStation.setLogos(logos);
            
            paStation.setId(id);
            
            Channels paChannels = new Channels();
            for (ChannelInfo channel : channels) {
                paChannels.getChannel().add(channel.createPaChannel());
            }
            
            paStation.setChannels(paChannels);
            
            return paStation;
        }
    }
}
