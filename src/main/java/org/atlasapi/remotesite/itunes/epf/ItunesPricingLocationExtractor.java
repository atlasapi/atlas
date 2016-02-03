package org.atlasapi.remotesite.itunes.epf;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Locale;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

public class ItunesPricingLocationExtractor implements ContentExtractor<ItunesEpfPricingSource, Maybe<Location>> {

    @Override
    public Maybe<Location> extract(ItunesEpfPricingSource source) {
        if (source.getRow().get(EpfPricing.STOREFRONT_ID) == source.getCountry()) {
            BigDecimal sdPrice = source.getRow().get(EpfPricing.SD_PRICE);
            if (sdPrice == null) {
                return Maybe.nothing();
            }

            Country country = convertCountry(source.getCountry());

            Location location = new Location();
            location.setTransportType(TransportType.APPLICATION);
            location.setTransportSubType(TransportSubType.ITUNES);
            location.setEmbedId(source.getRow().get(EpfPricing.VIDEO_ID).toString());

            Policy policy = new Policy();
            policy.addAvailableCountry(country);
            policy.setRevenueContract(RevenueContract.PAY_TO_BUY);

            Currency currency = Currency.getInstance(new Locale("en", country.code()));
            policy.setPrice(new Price(currency, sdPrice.movePointRight(currency.getDefaultFractionDigits()).intValue()));

            location.setPolicy(policy);

            return Maybe.just(location);
        } else {
            return Maybe.nothing();
        }
    }

    private Country convertCountry(int storefrontId) {
        if (storefrontId == ItunesEpfPricingSource.GB_CODE) {
            return Countries.GB;
        } else if (storefrontId == ItunesEpfPricingSource.US_CODE) {
            return Countries.US;
        } else {
            return Countries.ALL;
        }
    }

}
