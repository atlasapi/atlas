package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Pricing;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.Currency;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodVersionsExtractor {

    private static final String BT_VOD_GUID_NAMESPACE = "bt:vod:guid";
    private static final String BT_VOD_ID_NAMESPACE = "bt:vod:id";
    private static final String HD_FLAG = "HD";
    private static final String SD_FLAG = "SD";

    private final BtVodPricingAvailabilityGrouper grouper;
    private final String uriPrefix;


    public BtVodVersionsExtractor(BtVodPricingAvailabilityGrouper grouper, String uriPrefix) {
        this.grouper = checkNotNull(grouper);
        this.uriPrefix = checkNotNull(uriPrefix);
    }

    public Set<Version> createVersions(BtVodEntry row) {
        Set<Alias> aliases = ImmutableSet.of(
                new Alias(BT_VOD_GUID_NAMESPACE, row.getGuid()),
                new Alias(BT_VOD_ID_NAMESPACE, row.getId())
        );

        if (row.getProductOfferStartDate() == null
                || row.getProductOfferEndDate() == null
                || !isItemTvodPlayoutAllowed(row)
                || !isItemMediaAvailableOnCdn(row)
                ) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        if (!row.getSubscriptionCodes().isEmpty() || BrandUriExtractor.SERIES_TYPE.equals(row.getProductType())) {
            DateTime availabilityStart = new DateTime(row.getProductOfferStartDate(), DateTimeZone.UTC);
            DateTime availabilityEnd = new DateTime(row.getProductOfferEndDate(), DateTimeZone.UTC);
            locations.add(createLocation(row, new Interval(availabilityStart, availabilityEnd),
                    ImmutableSet.<BtVodProductPricingTier>of(), Policy.RevenueContract.SUBSCRIPTION, aliases));
        }

        //TODO filter for blackout
        if (!row.getProductPricingPlan().getProductPricingTiers().isEmpty()) {
            if (row.getProductOfferingType().contains("-EST")) {
                locations.addAll(createLocations(row, Policy.RevenueContract.PAY_TO_BUY, aliases));
            } else {
                locations.addAll(createLocations(row, Policy.RevenueContract.PAY_TO_RENT, aliases));
            }
        }

        Encoding encoding = new Encoding();
        encoding.setAvailableAt(locations.build());

        setQualityOn(encoding, row);

        Version version = new Version();
        version.setManifestedAs(ImmutableSet.of(encoding));
        version.setCanonicalUri(uriFor(row));
        version.setAliases(aliases);
        if (row.getProductDuration() != null) {
            version.setDuration(Duration.standardSeconds(row.getProductDuration()));
        }

        return ImmutableSet.of(version);
    }

    public void setQualityOn(Encoding encoding, BtVodEntry entry) {
        if (HD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(true);
            encoding.setQuality(Quality.HD);
        } else if (SD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(false);
            encoding.setQuality(Quality.SD);
        }
    }

    private Set<Location> createLocations(BtVodEntry row, Policy.RevenueContract subscription, Set<Alias> aliases) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        Multimap<Interval, BtVodProductPricingTier> groupAvailabilityPeriods = grouper.groupAvailabilityPeriods(row);
        for (Map.Entry<Interval, Collection<BtVodProductPricingTier>> entry : groupAvailabilityPeriods.asMap().entrySet()) {
            locations.add(createLocation(row, entry.getKey(), entry.getValue(), subscription, aliases));
        }
        return locations.build();
    }

    private boolean isItemMediaAvailableOnCdn(BtVodEntry row) {
        return true;
        //return row.getServiceTypes().contains(OTG_PLATFORM);
    }

    private boolean isItemTvodPlayoutAllowed(BtVodEntry row) {
        return true;
        //return !FALSE.equals(row.getMasterAgreementOtgTvodPlay());
    }

    private Location createLocation(BtVodEntry row, Interval availability, Collection<BtVodProductPricingTier> pricingTiers,
                                    Policy.RevenueContract subscription, Set<Alias> aliases) {

        Policy policy = new Policy();
        policy.setAvailabilityStart(availability.getStart());
        policy.setAvailabilityEnd(availability.getEnd());
        ImmutableList.Builder<Pricing> pricings = ImmutableList.builder();
        for (BtVodProductPricingTier pricingTier : pricingTiers) {
            DateTime startDate = new DateTime(pricingTier.getProductAbsoluteStart(), DateTimeZone.UTC);
            DateTime endDate = new DateTime(pricingTier.getProductAbsoluteEnd(), DateTimeZone.UTC);
            Double amount;
            if (pricingTier.getProductAmounts().getGBP() == null) {
                amount = 0D;
            } else {
                amount = pricingTier.getProductAmounts().getGBP();
            }
            Price price = new Price(Currency.getInstance("GBP"), amount);
            pricings.add(new Pricing(startDate, endDate, price));
        }
        policy.setPricing(pricings.build());
        policy.setSubscriptionPackages(row.getSubscriptionCodes());
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));

        if (!row.getSubscriptionCodes().isEmpty()) {
            policy.setRevenueContract(Policy.RevenueContract.SUBSCRIPTION);
        } else {
            if (row.getProductOfferingType().contains("-EST")) {
                policy.setRevenueContract(Policy.RevenueContract.PAY_TO_BUY);
            } else {
                policy.setRevenueContract(Policy.RevenueContract.PAY_TO_RENT);
            }
        }
        Location location = new Location();
        location.setPolicy(policy);
        location.setCanonicalUri(uriFor(row, policy.getRevenueContract()));
        location.setUri(uriFor(row));
        location.setAliases(aliases);

        return location;

    }

    private String uriFor(BtVodEntry row) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id;
    }

    private String uriFor(BtVodEntry row, Policy.RevenueContract revenueContract) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id + "/" + revenueContract.toString();
    }
}
