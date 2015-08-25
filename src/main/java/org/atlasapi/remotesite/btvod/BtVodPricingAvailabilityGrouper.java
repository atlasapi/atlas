package org.atlasapi.remotesite.btvod;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.joda.time.Interval;

import com.google.api.client.util.Sets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;


public class BtVodPricingAvailabilityGrouper {

    public Multimap<Interval, BtVodProductPricingTier> groupAvailabilityPeriods(BtVodEntry entry) {
        
        ImmutableMap<Interval, BtVodProductPricingTier> intervalToTier = Maps.uniqueIndex(
                Iterables.filter(entry.getProductPricingPlan().getProductPricingTiers(), Predicates.not(IS_BLACKOUT)), 
                new Function<BtVodProductPricingTier, Interval>() {
        
                    @Override
                    public Interval apply(BtVodProductPricingTier tier) {
                        return new Interval(tier.getProductAbsoluteStart(), tier.getProductAbsoluteEnd());
                    }
            
        });
       
        return groupByContiguousAvailabilityPeriods(intervalToTier);
    }
    
    private Multimap<Interval, BtVodProductPricingTier> groupByContiguousAvailabilityPeriods(
            ImmutableMap<Interval, BtVodProductPricingTier> intervalToTier) {
        
        ImmutableMultimap.Builder<Interval, BtVodProductPricingTier> builder = ImmutableMultimap.builder();
        ImmutableList<Interval> sortedIntervals = ORDERING_BY_INTERVAL_START.immutableSortedCopy(intervalToTier.keySet());
        
        Interval activeCombinedInterval = null;
        Set<BtVodProductPricingTier> tiersForCurrentInterval = Sets.newHashSet();
        for (Interval interval : sortedIntervals) {
            if(activeCombinedInterval == null 
                    || activeCombinedInterval.abuts(interval)
                    || activeCombinedInterval.overlaps(interval)) {
                activeCombinedInterval = joinIntervals(activeCombinedInterval, interval);
            } else {
                builder.putAll(activeCombinedInterval, tiersForCurrentInterval);
                tiersForCurrentInterval = Sets.newHashSet();
                activeCombinedInterval = interval;
            }
            BtVodProductPricingTier tier = intervalToTier.get(interval);
            if (!Boolean.TRUE.equals(tier.getIsBlackout())) {
                tiersForCurrentInterval.add(tier);
            }
        }
        
        if (!tiersForCurrentInterval.isEmpty()) {
            builder.putAll(activeCombinedInterval, tiersForCurrentInterval);
        }
        return builder.build();
    }

    private Interval joinIntervals(@Nullable Interval i1, Interval i2) {
        if (i1 == null) {
            return i2;
        }
        return new Interval(i1.getStartMillis(), i2.getEndMillis());
    }

    private static Ordering<Interval> ORDERING_BY_INTERVAL_START = new Ordering<Interval>() {

        @Override
        public int compare(Interval left, Interval right) {
            if (left.getStartMillis() == right.getStartMillis()) {
                return 0;
            }
            return left.getStartMillis() < right.getStartMillis() ? -1 : 0;
        }
        
    };
    
    private static final Predicate<BtVodProductPricingTier> IS_BLACKOUT = new Predicate<BtVodProductPricingTier>() {

        @Override
        public boolean apply(BtVodProductPricingTier input) {
            return Boolean.TRUE.equals(input.getIsBlackout());
        }
    };
}
