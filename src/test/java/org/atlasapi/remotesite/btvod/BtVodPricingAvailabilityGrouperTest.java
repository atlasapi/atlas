package org.atlasapi.remotesite.btvod;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.Map.Entry;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingPlan;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Interval;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;


public class BtVodPricingAvailabilityGrouperTest {

    private final BtVodPricingAvailabilityGrouper grouper = new BtVodPricingAvailabilityGrouper();
    
    @Test
    public void testTierMergingTiersOverlap() {
        BtVodProductPricingTier tier1 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 1, 0, 0, 0), new DateTime(2015, DateTimeConstants.FEBRUARY, 15, 0, 0, 0), false);
        BtVodProductPricingTier tier2 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 12, 0, 0, 0), new DateTime(2015, DateTimeConstants.MARCH, 1, 0, 0, 0), false);
        
        Multimap<Interval, BtVodProductPricingTier> grouped = grouper.groupAvailabilityPeriods(vodEntryWithPricingTiers(ImmutableList.of(tier1, tier2)));
        
        Entry<Interval, Collection<BtVodProductPricingTier>> onlyElement = Iterables.getOnlyElement(grouped.asMap().entrySet());
        assertThat(onlyElement.getKey(), is(new Interval(tier1.getProductAbsoluteStart(), tier2.getProductAbsoluteEnd())));
        assertThat(onlyElement.getValue().size(), is(2));
    }
    
    @Test
    public void testTierMergingTiersAbut() {
        BtVodProductPricingTier tier1 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 1, 0, 0, 0), new DateTime(2015, DateTimeConstants.FEBRUARY, 15, 0, 0, 0), false);
        BtVodProductPricingTier tier2 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 15, 0, 0, 0), new DateTime(2015, DateTimeConstants.MARCH, 1, 0, 0, 0), false);
        
        Multimap<Interval, BtVodProductPricingTier> grouped = grouper.groupAvailabilityPeriods(vodEntryWithPricingTiers(ImmutableList.of(tier1, tier2)));
        
        Entry<Interval, Collection<BtVodProductPricingTier>> onlyElement = Iterables.getOnlyElement(grouped.asMap().entrySet());
        assertThat(onlyElement.getKey(), is(new Interval(tier1.getProductAbsoluteStart(), tier2.getProductAbsoluteEnd())));
        assertThat(onlyElement.getValue().size(), is(2));
    }
    
    @Test
    public void testTierMergingTiersNoOverlap() {
        BtVodProductPricingTier tier1 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 1, 0, 0, 0), new DateTime(2015, DateTimeConstants.FEBRUARY, 15, 0, 0, 0), false);
        BtVodProductPricingTier tier2 = tier(new DateTime(2015, DateTimeConstants.MARCH, 15, 0, 0, 0), new DateTime(2015, DateTimeConstants.APRIL, 1, 0, 0, 0), false);
        
        Multimap<Interval, BtVodProductPricingTier> grouped = grouper.groupAvailabilityPeriods(vodEntryWithPricingTiers(ImmutableList.of(tier1, tier2)));
        
        Collection<BtVodProductPricingTier> firstTierPeriod = grouped.asMap().get(new Interval(tier1.getProductAbsoluteStart(), tier1.getProductAbsoluteEnd()));
        assertThat(Iterables.getOnlyElement(firstTierPeriod).getProductAbsoluteStart(), is(tier1.getProductAbsoluteStart()));
        
        Collection<BtVodProductPricingTier> secondTierPeriod = grouped.asMap().get(new Interval(tier2.getProductAbsoluteStart(), tier2.getProductAbsoluteEnd()));
        assertThat(Iterables.getOnlyElement(secondTierPeriod).getProductAbsoluteStart(), is(tier2.getProductAbsoluteStart()));
    }
    
    @Test
    public void testBlackoutTiersAreRemoved() {
        BtVodProductPricingTier tier1 = tier(new DateTime(2015, DateTimeConstants.FEBRUARY, 1, 0, 0, 0), new DateTime(2015, DateTimeConstants.FEBRUARY, 15, 0, 0, 0), true);
        BtVodProductPricingTier tier2 = tier(new DateTime(2015, DateTimeConstants.MARCH, 15, 0, 0, 0), new DateTime(2015, DateTimeConstants.APRIL, 1, 0, 0, 0), false);
        
        Multimap<Interval, BtVodProductPricingTier> grouped = grouper.groupAvailabilityPeriods(vodEntryWithPricingTiers(ImmutableList.of(tier1, tier2)));
        
        assertThat(Iterables.getOnlyElement(grouped.values()).getProductAbsoluteStart(), is(tier2.getProductAbsoluteStart()));
    }
    
    private BtVodEntry vodEntryWithPricingTiers(Iterable<BtVodProductPricingTier> tiers) {
        BtVodEntry entry = new BtVodEntry();
        BtVodProductPricingPlan plan = new BtVodProductPricingPlan();
        plan.setProductPricingTiers(ImmutableList.copyOf(tiers));
        entry.setProductPricingPlan(plan);
        
        return entry;
    }
    
    private BtVodProductPricingTier tier(DateTime start, DateTime end, boolean blackout) {
        BtVodProductPricingTier tier = new BtVodProductPricingTier();
        tier.setProductAbsoluteStart(start.getMillis());
        tier.setProductAbsoluteEnd(end.getMillis());
        tier.setIsBlackout(blackout);
        return tier;
    }
}
