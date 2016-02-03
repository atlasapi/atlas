package org.atlasapi.remotesite.itunes.epf;

import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;

import com.metabroadcast.common.intl.Country;

public class ItunesEpfPricingSource {

    private final EpfPricing row;
    private final int country;

    public static final int GB_CODE = 143444;
    public static final int US_CODE = 143441;
    
    public ItunesEpfPricingSource(EpfPricing row, int country) {
        this.row = row;
        this.country = country;
    }
    
    public EpfPricing getRow() {
        return this.row;
    }
    public int getCountry() {
        return this.country;
    }
    
}
