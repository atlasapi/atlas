package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Currency;
import java.util.Map;
import java.util.Set;

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
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;

public class BtVodVersionsExtractor {

    private static final String OTG_PLATFORM = "OTG";
    private static final String HD_FLAG = "HD";
    private static final String SD_FLAG = "SD";
    private static final String FOUR_K = "4K";
    public static final String PAY_TO_BUY_SUFFIX = "-EST";
    public static final String PRERELEASE_BLACKOUT_TYPE = "prerelease";

    private final String uriPrefix;
    private final BtVodPricingAvailabilityGrouper grouper;
    private final String guidAliasNamespace;
    private final String idAliasNamespace;
    private final Long btTvServiceId;
    private final Long btTvOtgServiceId;


    public BtVodVersionsExtractor(
            BtVodPricingAvailabilityGrouper grouper,
            String uriPrefix,
            String guidAliasNamespace,
            String idAliasNamespace, 
            Long btTvServiceId, 
            Long btTvOtgServiceId
    ) {
        this.grouper = checkNotNull(grouper);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.guidAliasNamespace = checkNotNull(guidAliasNamespace);
        this.idAliasNamespace = checkNotNull(idAliasNamespace);
        this.btTvServiceId = btTvServiceId;
        this.btTvOtgServiceId = btTvOtgServiceId;
    }

    public Set<Version> createVersions(BtVodEntry row) {
        if (isBlackout(row)) {
            return ImmutableSet.of();
        }

        Set<Alias> aliases = ImmutableSet.of(
                new Alias(guidAliasNamespace, row.getGuid()),
                new Alias(idAliasNamespace, row.getId())
        );

        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        
        locations.addAll(createLocations(row, btTvServiceId, aliases));
        
        if (hasServiceTypeOtg(row)) {
            locations.addAll(createLocations(row, btTvOtgServiceId, aliases));
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

    private boolean isBlackout(BtVodEntry row) {
        if (row.getProductPricingPlan() == null
                || row.getProductPricingPlan().getProductPricingTiers().isEmpty()) {
            return false;
        }

        for (BtVodProductPricingTier pricingTier :
                row.getProductPricingPlan().getProductPricingTiers()) {
            if (isBlackout(pricingTier)) {
                return true;
            }
        }
        return false;
    }

    private boolean isBlackout(BtVodProductPricingTier pricingTier) {
        DateTime now = DateTime.now().withZone(DateTimeZone.UTC);

        DateTime start = new DateTime(pricingTier.getProductAbsoluteStart(), DateTimeZone.UTC);
        DateTime end = new DateTime(pricingTier.getProductAbsoluteEnd(), DateTimeZone.UTC);

        return Boolean.TRUE.equals(pricingTier.getIsBlackout())
                && PRERELEASE_BLACKOUT_TYPE.equals(pricingTier.getBlackoutType())
                && isInRange(start, now, end);
    }

    private boolean isInRange(DateTime start, DateTime dateTime, DateTime end) {
        return (start.isBefore(dateTime) || start.isEqual(dateTime))
                && (end.isAfter(dateTime) || end.isEqual(dateTime));
    }
    
    private Iterable<Location> createLocations(BtVodEntry row, Long serviceId, Set<Alias> aliases) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        
        if (!row.getSubscriptionCodes().isEmpty()) {
            DateTime availabilityStart = new DateTime(row.getProductOfferStartDate(), DateTimeZone.UTC);
            DateTime availabilityEnd = new DateTime(row.getProductOfferEndDate(), DateTimeZone.UTC);
            Optional<Location> location = createLocation(
                    row,
                    new Interval(availabilityStart, availabilityEnd),
                    ImmutableSet.<BtVodProductPricingTier>of(),
                    Policy.RevenueContract.SUBSCRIPTION,
                    aliases,
                    serviceId,
                    row.getSubscriptionCodes()
            );
            if(location.isPresent()) {
                locations.add(location.get());
            }
        }

        if (row.getProductPricingPlan() != null && !row.getProductPricingPlan().getProductPricingTiers().isEmpty()) {
            if (row.getProductOfferingType() != null
                    && row.getProductOfferingType().contains(PAY_TO_BUY_SUFFIX)) {
                locations.addAll(createLocations(row, Policy.RevenueContract.PAY_TO_BUY, aliases, serviceId));
            } else {
                locations.addAll(createLocations(row, Policy.RevenueContract.PAY_TO_RENT, aliases, serviceId));
            }
        }
        return locations.build();
    }

    public void setQualityOn(Encoding encoding, BtVodEntry entry) {
        if (HD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(true);
            encoding.setQuality(Quality.HD);
        } else if (SD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(false);
            encoding.setQuality(Quality.SD);
        } else if (FOUR_K.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(true);
            encoding.setQuality(Quality.FOUR_K);
        }
    }

    private Set<Location> createLocations(BtVodEntry row, Policy.RevenueContract subscription, Set<Alias> aliases, Long serviceId) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        Multimap<Interval, BtVodProductPricingTier> groupAvailabilityPeriods = grouper.groupAvailabilityPeriods(row);
        for (Map.Entry<Interval, Collection<BtVodProductPricingTier>> entry : groupAvailabilityPeriods.asMap().entrySet()) {
            Optional<Location> location = createLocation(
                    row,
                    entry.getKey(),
                    entry.getValue(),
                    subscription,
                    aliases,
                    serviceId,
                    ImmutableSet.<String>of()
            );
            if(location.isPresent()) {
                locations.add(location.get());
            }
        }
        
        if (groupAvailabilityPeriods.asMap().entrySet().isEmpty()) {
            Optional<Location> location = createLocation(
                    row,
                    null,
                    ImmutableSet.<BtVodProductPricingTier>of(),
                    subscription,
                    aliases,
                    serviceId,
                    ImmutableSet.<String>of()
            );
            if(location.isPresent()) {
                locations.add(location.get());
            }
        }
        return locations.build();
    }

    private boolean hasServiceTypeOtg(BtVodEntry row) {
        return row.getServiceTypes().contains(OTG_PLATFORM);
                // TODO Change postponed due to proximity to launch
                // && row.getMasterAgreementServiceTypes().contains(OTG_PLATFORM);
    }

    private boolean isItemTvodPlayoutAllowed(BtVodEntry row) {
        return !Boolean.FALSE.equals(Boolean.valueOf(row.getMasterAgreementOtgTvodPlay()));
    }

    private Optional<Location> createLocation(BtVodEntry row, Interval availability,
            Collection<BtVodProductPricingTier> pricingTiers, Policy.RevenueContract subscription,
            Set<Alias> aliases, Long serviceId, ImmutableSet<String> subscriptionCodes) {
        Policy policy = new Policy();
        if (availability != null) {
            policy.setAvailabilityStart(availability.getStart());
            policy.setAvailabilityEnd(availability.getEnd());
        } else {
            policy.setAvailabilityStart(new DateTime(2000, DateTimeConstants.JANUARY, 1, 0, 0, 0));
            policy.setAvailabilityEnd(new DateTime(2035, DateTimeConstants.JANUARY, 1, 0, 0, 0));
        }
        policy.setService(serviceId);
        ImmutableList.Builder<Pricing> pricingsBuilder = ImmutableList.builder();
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

            if(!policyIsPayToX(subscription) || price.getAmount() > 0) {
                pricingsBuilder.add(new Pricing(startDate, endDate, price));
            }
        }

        ImmutableList<Pricing> pricings = pricingsBuilder.build();
        if(policyIsPayToX(subscription) && pricings.isEmpty()) {
            return Optional.absent();
        }

        policy.setPricing(pricings);
        policy.setSubscriptionPackages(subscriptionCodes);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setRevenueContract(subscription);

        Location location = new Location();
        location.setPolicy(policy);
        location.setCanonicalUri(uriFor(row, policy.getRevenueContract(), serviceId));
        location.setUri(uriFor(row));
        location.setAliases(aliases);

        return Optional.of(location);
    }

    private String uriFor(BtVodEntry row) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id;
    }

    private String uriFor(BtVodEntry row, Policy.RevenueContract revenueContract, Long serviceId) {
        String id = row.getGuid();
        String serviceIdString = serviceId != null ? serviceId.toString() : "";
        return uriPrefix + "items/" + id + "/" + revenueContract.toString()
                + "/" + serviceIdString;
    }

    private boolean policyIsPayToX(Policy.RevenueContract subscription) {
        return subscription == Policy.RevenueContract.PAY_TO_RENT
                || subscription == Policy.RevenueContract.PAY_TO_BUY;
    }
}
