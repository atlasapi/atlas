package org.atlasapi.output.simple;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Currency;
import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;

public class ItemModelSimplifierTest {

    private static final String VERSION_ALIAS_VALUE = "value";
    private static final String VERSION_ALIAS_NAMESPACE = "namespace";
    private static final long BBC_ONE_PARENT = 105256L;
    private static final long BBC_ONE_HD = 103828L;
    
    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Mockery context = new Mockery();
    private final ContentGroupResolver contentGroupResolver = context.mock(ContentGroupResolver.class);
    private final TopicQueryResolver topicResolver = context.mock(TopicQueryResolver.class);
    private final SegmentResolver segmentResolver = context.mock(SegmentResolver.class);
    private final ProductResolver productResolver = context.mock(ProductResolver.class);
    private final ContainerSummaryResolver containerSummaryResolver = context.mock(ContainerSummaryResolver.class);
    private final ChannelResolver channelResolver = context.mock(ChannelResolver.class);
    private final PeopleQueryResolver peopleQueryResolver = context.mock(PeopleQueryResolver.class);
    private final UpcomingItemsResolver upcomingResolver = context.mock(UpcomingItemsResolver.class);
    private final AvailableItemsResolver availableResolver = context.mock(AvailableItemsResolver.class);
    private final ChannelGroupResolver channelGroupResolver = context.mock(ChannelGroupResolver.class);
    private final ChannelSimplifier channelSimplifier = new ChannelSimplifier(codec, codec, channelResolver, new PublisherSimplifier(), new ImageSimplifier(), new ChannelGroupSummarySimplifier(codec, channelGroupResolver), channelGroupResolver);
    private final ItemModelSimplifier itemSimplifier = new ItemModelSimplifier("localHostName", contentGroupResolver, topicResolver, productResolver, segmentResolver, containerSummaryResolver, channelResolver, new SubstitutionTableNumberCodec(), new SubstitutionTableNumberCodec(), new ImageSimplifier(), peopleQueryResolver,upcomingResolver,availableResolver,null, null, null, channelSimplifier, null, null, null);
    
    @Test
    @SuppressWarnings("unchecked")
    public void testCanCreateSimpleItemFromFullItem() throws Exception {
        final ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Image channelImage = Image.builder("http://example.com/image").withTheme(ImageTheme.LIGHT_OPAQUE).build();
        
        final Channel channel = new Channel(Publisher.BBC, "test", "a", true, MediaType.VIDEO, "http://example.com/");
        channel.setId(BBC_ONE_HD);
        channel.setImages(ImmutableSet.of(new TemporalField<Image>(channelImage, new LocalDate(), null)));
        
        final Channel parent = new Channel(Publisher.BBC, "parent", "b", true, MediaType.VIDEO, "http://example.com/parent");
        parent.setId(BBC_ONE_PARENT);
        channel.setParent(parent);
        
        context.checking(new Expectations(){{
            allowing(contentGroupResolver).findByIds(with(any(Iterable.class)));will(returnValue(ImmutableList.of()));
            allowing(topicResolver).topicsForIds(with(any(Iterable.class)));will(returnValue(ImmutableList.of()));
            allowing(segmentResolver).resolveById(with(any(Iterable.class)));will(returnValue(ImmutableMap.of()));
            allowing(channelResolver).fromUri(with(channel.getCanonicalUri()));will(returnValue(Maybe.just(channel)));
            allowing(channelResolver).fromId(with(105256L));will(returnValue(Maybe.just(parent)));
            allowing(channelResolver).fromId(with(103828L));will(returnValue(Maybe.just(channel)));
        }});
        
        org.atlasapi.media.entity.Item fullItem = new org.atlasapi.media.entity.Item();
        Broadcast broadcast = ComplexBroadcastTestDataBuilder.broadcast().withChannel(channel.getCanonicalUri()).build();

        LocalDate releaseDateDate = new LocalDate(DateTimeZone.UTC);
        ReleaseDate releaseDate = new ReleaseDate(releaseDateDate, Countries.ALL, ReleaseDate.ReleaseType.GENERAL);
        fullItem.setReleaseDates(Lists.newArrayList(releaseDate));

        Version version = new Version();
        version.addBroadcast(broadcast);
        
        Restriction restriction = new Restriction();
        restriction.setRestricted(true);
        restriction.setMessage("adults only");
        version.setRestriction(restriction);
        
        Encoding encoding = new Encoding();
        encoding.setDataContainerFormat(MimeType.VIDEO_3GPP);
        encoding.setSubtitled(true);
        version.addManifestedAs(encoding);
        version.addAlias(new Alias(VERSION_ALIAS_NAMESPACE, VERSION_ALIAS_VALUE));
        
        Location location = new Location();
        location.setUri("http://example.com");
        location.setPolicy(new Policy().withRevenueContract(RevenueContract.PAY_TO_BUY).withPrice(new Price(Currency.getInstance("GBP"), 99)).withAvailableCountries(Countries.GB));
        Location embed = new Location();
        embed.setTransportType(TransportType.EMBED);
        embed.setEmbedId("embedId");
        embed.setTransportSubType(TransportSubType.BRIGHTCOVE);
        
        encoding.addAvailableAt(location);
        encoding.addAvailableAt(embed);
        encoding.setAudioDescribed(true);
        encoding.setHighDefinition(true);
        fullItem.addVersion(version);
        fullItem.setTitle("Collings and Herrin");
        fullItem.setShortDescription("Hello");
        fullItem.setMediumDescription("Hello World");
        fullItem.setLongDescription("Hello World Test");

        CrewMember person = Actor.actor("hisID", "Andrew Collings", "Dirt-bag Humperdink", Publisher.BBC);
        fullItem.addPerson(person);
        
        Item simpleItem = itemSimplifier
                            .simplify
                              (
                                fullItem, 
                                Sets.union(Annotation.defaultAnnotations(), 
                                           ImmutableSet.of(Annotation.CHANNEL_SUMMARY, Annotation.V4_ALIASES)), 
                                config
                              );
        
        List<org.atlasapi.media.entity.simple.Person> people = simpleItem.getPeople();
        org.atlasapi.media.entity.simple.Person simpleActor = Iterables.getOnlyElement(people);
        assertThat(simpleActor.getCharacter(), is("Dirt-bag Humperdink"));
        assertThat(simpleActor.getName(), is("Andrew Collings"));
        
        Set<org.atlasapi.media.entity.simple.Location> simpleLocations = simpleItem.getLocations();
        assertThat(simpleLocations.size(), is(2));
        org.atlasapi.media.entity.simple.Location simpleLocation = Iterables.getFirst(simpleLocations, null);
        
        assertThat(simpleLocation.getUri(), is("http://example.com"));
        assertThat(simpleLocation.getDataContainerFormat(), is(MimeType.VIDEO_3GPP.toString()));
        assertThat(simpleLocation.getRestriction().getMessage(), is("adults only"));
        assertThat(simpleLocation.getRevenueContract(), is("pay_to_buy"));
        assertThat(simpleLocation.getCurrency(), is("GBP"));
        assertThat(simpleLocation.getPrice(), is(99));
        assertThat(simpleLocation.getAvailableCountries().size(), is(1));
        assertThat(simpleLocation.getAvailableCountries().iterator().next(), is("GB"));
        assertThat(simpleLocation.getAudioDescribed(), is(true));
        assertThat(simpleLocation.getSubtitled(), is(true));
        assertThat(Iterables.getOnlyElement(simpleLocation.getV4Aliases()).getNamespace(), is(VERSION_ALIAS_NAMESPACE));
        assertThat(Iterables.getOnlyElement(simpleLocation.getV4Aliases()).getValue(), is(VERSION_ALIAS_VALUE));
        
        org.atlasapi.media.entity.simple.Location simpleEmbed = Iterables.getLast(simpleLocations, null);
        assertThat(simpleEmbed.getEmbedId(), is("embedId"));
        assertThat(simpleEmbed.getTransportType(), is("embed"));
        assertThat(simpleEmbed.getTransportSubType(), is("brightcove"));
        assertThat(simpleEmbed.getHighDefinition(), is(true));

        assertThat(simpleItem.getTitle(), is("Collings and Herrin"));
        assertThat(simpleItem.getShortDescription(), is("Hello"));
        assertThat(simpleItem.getMediumDescription(), is("Hello World"));
        assertThat(simpleItem.getLongDescription(), is("Hello World Test"));
        assertThat(Iterables.getOnlyElement(simpleItem.getReleaseDates()).getDate().getDay(), is(releaseDateDate.toDate().getDay()));
        
        org.atlasapi.media.entity.simple.Channel broadcastChannel = Iterables.getOnlyElement(simpleItem.getBroadcasts()).getChannel();
        
        assertThat(broadcastChannel.getTitle(), is(channel.getTitle()));
        assertThat(broadcastChannel.getParent().getTitle(), is(parent.getTitle()));
        assertThat(Iterables.getOnlyElement(broadcastChannel.getParent().getImages()).getUri(), is(channelImage.getCanonicalUri()));
    }

    @Test
    public void testCanCreateSimpleItemTypeEpisodefromFullComplexEpisode() {
        org.atlasapi.media.entity.Item fullItem = new org.atlasapi.media.entity.Episode();

        LocalDate releaseDateDate = new LocalDate(DateTimeZone.UTC);
        ReleaseDate releaseDate = new ReleaseDate(releaseDateDate, Countries.ALL, ReleaseDate.ReleaseType.GENERAL);
        fullItem.setReleaseDates(Lists.newArrayList(releaseDate));

        Version version = new Version();

        Restriction restriction = new Restriction();
        restriction.setRestricted(true);
        restriction.setMessage("adults only");

        org.atlasapi.media.entity.simple.Restriction simpleRestriction = new org.atlasapi.media.entity.simple.Restriction();
        simpleRestriction.setRestricted(true);
        simpleRestriction.setMessage("adults only");

        version.setRestriction(restriction);

        Encoding encoding = new Encoding();
        encoding.setDataContainerFormat(MimeType.VIDEO_3GPP);
        encoding.setSubtitled(true);
        encoding.setHighDefinition(true);
        version.addManifestedAs(encoding);
        version.addAlias(new Alias(VERSION_ALIAS_NAMESPACE, VERSION_ALIAS_VALUE));

        Location location = new Location();
        location.setUri("http://example.com");
        location.setPolicy(new Policy().withRevenueContract(RevenueContract.PAY_TO_BUY).withPrice(new Price(Currency.getInstance("GBP"), 99)).withAvailableCountries(Countries.GB));
        Location embed = new Location();
        embed.setTransportType(TransportType.EMBED);
        embed.setEmbedId("embedId");
        embed.setTransportSubType(TransportSubType.BRIGHTCOVE);

        encoding.addAvailableAt(location);
        encoding.addAvailableAt(embed);
        encoding.setAudioDescribed(true);
        fullItem.addVersion(version);
        fullItem.setTitle("Collings and Herrin");
        fullItem.setShortDescription("Hello");
        fullItem.setShortDescription("Hello World");
        fullItem.setShortDescription("Hello World Test");

        final ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();

        Item simpleItem = itemSimplifier
                .simplify
                        (
                                fullItem,
                                Sets.union(Annotation.defaultAnnotations(),
                                        ImmutableSet.of(Annotation.CHANNEL_SUMMARY, Annotation.V4_ALIASES)),
                                config
                        );

        assertThat(simpleItem.getShortDescription(), is(fullItem.getShortDescription()));
        assertThat(simpleItem.getMediumDescription(), is(fullItem.getMediumDescription()));
        assertThat(simpleItem.getLongDescription(), is(fullItem.getLongDescription()));
        assertThat(simpleItem.getTitle(), is(fullItem.getTitle()));

        Set<org.atlasapi.media.entity.simple.Location> simpleLocations = simpleItem.getLocations();
        org.atlasapi.media.entity.simple.Location simpleLocation = simpleLocations.iterator().next();

        assertThat(simpleLocation.getHighDefinition(), is(encoding.getHighDefinition()));
        assertThat(simpleLocation.getRestriction().getMessage(), is(simpleRestriction.getMessage()));
    }
}
