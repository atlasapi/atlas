package org.atlasapi.remotesite.itunes.epf;

import java.util.Map;

import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;

import com.metabroadcast.common.intl.Country;

public class ItunesEpfPricingSource {

    private final EpfPricing row;
    private final Country country;
    private final Map<String, Integer> countryCodes;
    
    public ItunesEpfPricingSource(EpfPricing row, Country country, Map<String, Integer> countryCodes) {
        this.row = row;
        this.country = country;
        this.countryCodes = countryCodes;
    }
    
    public EpfPricing getRow() {
        return this.row;
    }
    public Country getCountry() {
        return this.country;
    }
    public Map<String, Integer> getCountryCodes() {
        return this.countryCodes;
    }
    
}
