package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnpublishedContentFilterTest {

    private final UnpublishedContentFilter<Item> filter = new UnpublishedContentFilter<Item>();

    @Test
    public void testFiltersUnpublishedContent() {
        Item item = new Item();
        item.setCanonicalUri("uri");
        item.setActivelyPublished(false);
        ScoredCandidate<Item> itemScoredCandidate = ScoredCandidate.valueOf(item, Score.ONE);
        Item subject = new Item();
        subject.setCanonicalUri("subject");
        assertFalse(filter.doFilter(itemScoredCandidate, subject, new DefaultDescription()));
    }

    @Test
    public void testDoesntFilterActivelyPublishedContent() {
        Item item = new Item();
        item.setCanonicalUri("uri");
        ScoredCandidate<Item> itemScoredCandidate = ScoredCandidate.valueOf(item, Score.ONE);
        Item subject = new Item();
        subject.setCanonicalUri("subject");
        assertTrue(filter.doFilter(itemScoredCandidate, subject, new DefaultDescription()));
    }
}