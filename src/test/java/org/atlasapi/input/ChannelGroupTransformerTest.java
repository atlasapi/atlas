package org.atlasapi.input;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.Alias;
import org.atlasapi.media.entity.simple.ChannelGroup;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChannelGroupTransformerTest {

    private ChannelGroup simpleChannelGroup;
    private ChannelGroupTransformer transformer;
    private PublisherDetails publisherDetails;

    @Before
    public void setUp() {
        simpleChannelGroup = mock(ChannelGroup.class);

        transformer = new ChannelGroupTransformer();
        publisherDetails = new PublisherDetails(Publisher.BBC.key());
    }

    @Test
    public void testChannelGroupIsTransformedCorrectly() throws Exception {
        when(simpleChannelGroup.getId()).thenReturn("pnd");
        when(simpleChannelGroup.getPublisherDetails()).thenReturn(publisherDetails);
        when(simpleChannelGroup.getTitle()).thenReturn("panda");
        when(simpleChannelGroup.getUri()).thenReturn("uri");
        when(simpleChannelGroup.getV4Aliases()).thenReturn(ImmutableSet.of(
                new Alias("bt:subscription-code", "SC1")
        ));

        org.atlasapi.media.channel.ChannelGroup complexChannelGroup = transformer.transform(simpleChannelGroup);

        assertEquals(22515, (long) complexChannelGroup.getId());
        assertEquals("uri", complexChannelGroup.getCanonicalUri());
        assertEquals(Publisher.BBC, complexChannelGroup.getPublisher());
        assertEquals("panda", complexChannelGroup.getTitle());
        assertEquals("SC1", complexChannelGroup.getAliases().iterator().next().getValue());
    }

    @Test(expected = NullPointerException.class)
    public void testChannelGroupDoesntHaveId() throws Exception {
        when(simpleChannelGroup.getPublisherDetails()).thenReturn(publisherDetails);

        org.atlasapi.media.channel.ChannelGroup complexChannelGroup = transformer.transform(simpleChannelGroup);

        assertTrue(complexChannelGroup.getId() == null);
    }

}
