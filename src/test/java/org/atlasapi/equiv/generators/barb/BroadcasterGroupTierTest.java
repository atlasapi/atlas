package org.atlasapi.equiv.generators.barb;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcasterGroupTierTest {

    @Test
    public void testBbcContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:1:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testItvContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:2:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testC4ContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:3:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testC5ContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:4:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testUktvContentIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:63:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testSkyContentIsNotTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(new Alias("gb:barb:broadcastGroup:5:transmission", "")));
        assertFalse(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testContentWithMixedAliasesIsTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases())
                .thenReturn(ImmutableSet.of(
                        new Alias("gb:barb:broadcastGroup:1:transmission", ""),
                        new Alias("gb:barb:broadcastGroup:5:transmission", "")));
        assertTrue(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testContentWithNonBgidAliasesIsNotTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases()).thenReturn(ImmutableSet.of(new Alias("pa:brand", "")));
        assertFalse(BroadcasterGroupTier.hasTierOneAlias(content));
    }

    @Test
    public void testContentWithNoAliasesIsNotTierOne() {
        Content content = mock(Content.class);
        when(content.getAliases()).thenReturn(ImmutableSet.of());
        assertFalse(BroadcasterGroupTier.hasTierOneAlias(content));
    }
}