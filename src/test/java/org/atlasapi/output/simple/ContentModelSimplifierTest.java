package org.atlasapi.output.simple;

import static org.junit.Assert.assertEquals;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.persistence.output.RecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.junit.Test;
import org.mockito.Mockito;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class ContentModelSimplifierTest {
    
    private ApplicationConfiguration applicationConfig = Mockito.mock(ApplicationConfiguration.class);

    @SuppressWarnings("unchecked")
    private final ContainerModelSimplifier containerSimplifier = new ContainerModelSimplifier(
        Mockito.mock(ModelSimplifier.class), 
        "", 
        Mockito.mock(ContentGroupResolver.class), 
        Mockito.mock(TopicQueryResolver.class), 
        Mockito.mock(AvailableItemsResolver.class), 
        Mockito.mock(UpcomingItemsResolver.class), 
        Mockito.mock(ProductResolver.class), 
        Mockito.mock(RecentlyBroadcastChildrenResolver.class), 
        Mockito.mock(ImageSimplifier.class),
        Mockito.mock(PeopleQueryResolver.class),
        Mockito.mock(ContainerSummaryResolver.class),
        Mockito.mock(EventRefModelSimplifier.class)
    );
    
    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelSimplifier channelSimplifier = new ChannelSimplifier(codec, codec, Mockito.mock(ChannelResolver.class), new PublisherSimplifier(), new ImageSimplifier(), new ChannelGroupSummarySimplifier(codec, Mockito.mock(ChannelGroupResolver.class)), Mockito.mock(ChannelGroupResolver.class));
    private final ItemModelSimplifier itemSimplifier = new ItemModelSimplifier(
        "", 
        Mockito.mock(ContentGroupResolver.class), 
        Mockito.mock(TopicQueryResolver.class), 
        Mockito.mock(ProductResolver.class), 
        Mockito.mock(SegmentResolver.class), 
        Mockito.mock(ContainerSummaryResolver.class), 
        Mockito.mock(ChannelResolver.class), 
        Mockito.mock(NumberToShortStringCodec.class), 
        new SubstitutionTableNumberCodec(), 
        Mockito.mock(ImageSimplifier.class),
        Mockito.mock(PeopleQueryResolver.class),
        Mockito.mock(UpcomingItemsResolver.class),
        Mockito.mock(AvailableItemsResolver.class),
        null,
        null,
        null,
        channelSimplifier,
        null,
        null,
        Mockito.mock(EventRefModelSimplifier.class)
    );

    @Test
    public void testUsesLowerCaseIdCodec() {
        Container container = new Container();
        container.setId(1234l);
        containerSimplifier.exposeIds(true);
        Playlist simplified = containerSimplifier.simplify(container, Annotation.defaultAnnotations(), applicationConfig);
        String lowerCasedId = simplified.getId().toLowerCase();
        assertEquals(lowerCasedId, simplified.getId());
        
        Item item = new Item();
        item.setId(1234l);
        itemSimplifier.exposeIds(true);
        org.atlasapi.media.entity.simple.Item simpleItem = itemSimplifier.simplify(item , Annotation.defaultAnnotations(), applicationConfig);
        lowerCasedId = simplified.getId().toLowerCase();
        assertEquals(lowerCasedId, simpleItem.getId());
    }

}
