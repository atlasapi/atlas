package org.atlasapi.remotesite.itunes.epf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.Currency;
import java.util.Locale;

import junit.framework.TestCase;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;

public class ItunesPricingLocationExtractorTest extends TestCase {

    private final ItunesPricingLocationExtractor extractor = new ItunesPricingLocationExtractor();

    @Test
    public void testExtract() {

        EpfPricing pricing = new EpfPricing(ImmutableList.of(
            "1453888800445","719590104","1.49","EUR","143444","","1.49","","","",""
        ));

        Maybe<Location> extractedLocation = extractor.extract(new ItunesEpfPricingSource(pricing, ItunesEpfPricingSource.GB_CODE));
        
        assertTrue(extractedLocation.hasValue());
        
        Location location = extractedLocation.requireValue();

        assertThat(location.getTransportType(), is(TransportType.APPLICATION));
        assertThat(location.getTransportSubType(), is(TransportSubType.ITUNES));
        assertThat(location.getEmbedId(), is(String.valueOf(719590104)));
        
        Policy policy = location.getPolicy();
        
        assertThat(policy.getAvailableCountries().size(), is(1));
        assertThat(policy.getAvailableCountries(), hasItem(Countries.GB));
        assertThat(policy.getRevenueContract(), is(RevenueContract.PAY_TO_BUY));
        assertThat(policy.getPrice(), is(new Price(Currency.getInstance(Locale.UK), 149)));
        
    }

}
