package org.atlasapi.equiv.generators.barb;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import org.junit.Test;

import static org.atlasapi.equiv.generators.barb.TieredBroadcaster.TXLOG_BROADCASTER_GROUP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcasterGroupTierTest {

    @Test
    public void testBbcContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.BBC_NITRO);
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testBbcBgidIsTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("1");
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testItvContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.ITV_CPS);
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testItvBgidIsTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("2");
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testC4ContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.C4_PMLSD);
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testC4BgidIsTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("3");
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testC5ContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.C5_DATA_SUBMISSION);
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testC5BgidIsTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("4");
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testUktvContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.UKTV);
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testUktvBgidIsTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("63");
        assertTrue(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testImdbContentIsNotTierOne() {
        Content content = mock(Content.class);
        when(content.getPublisher()).thenReturn(Publisher.IMDB);
        assertFalse(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testSkyBgidIsNotTierOne() {
        Content content = mock(Content.class);
        when(content.getCustomField(TXLOG_BROADCASTER_GROUP)).thenReturn("5");
        assertFalse(TieredBroadcaster.isTierOne(content));
    }

    @Test
    public void testContentWithNoSourceNorBgidIsNotTierOne() {
        Content content = mock(Content.class);
        assertFalse(TieredBroadcaster.isTierOne(content));
    }
}