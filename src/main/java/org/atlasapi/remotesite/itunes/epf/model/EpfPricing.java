package org.atlasapi.remotesite.itunes.epf.model;

import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.BIG_DECIMAL;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.DATE_TIME;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.INTEGER;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.STRING;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.TIMESTAMP;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.column;

import java.math.BigDecimal;
import java.util.List;

import com.metabroadcast.common.time.Timestamp;

import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Objects;

public class EpfPricing extends EpfTableRow {

    public static Function<List<String>, EpfPricing> FROM_ROW_PARTS = new Function<List<String>, EpfPricing>() {
        @Override
        public EpfPricing apply(List<String> input) {
            return new EpfPricing(input);
        }
    };
    
    public EpfPricing(List<String> rowParts) {
        super(rowParts);
    }

    private static int iota = 0;
    public static final EpfTableColumn<Timestamp> EXPORT_DATE = column(iota++, TIMESTAMP);
    public static final EpfTableColumn<Integer> VIDEO_ID = column(iota++, INTEGER);
    public static final EpfTableColumn<BigDecimal> RETAIL_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<String> CURRENCY_CODE = column(iota++, STRING);
    public static final EpfTableColumn<Integer> STOREFRONT_ID = column(iota++, INTEGER);
    public static final EpfTableColumn<DateTime> AVAILABILITY_DATE = column(iota++, DATE_TIME);
    public static final EpfTableColumn<BigDecimal>  SD_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<BigDecimal>  HQ_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<BigDecimal> LC_RENTAL_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<BigDecimal> SD_RENTAL_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<BigDecimal> HD_RENTAL_PRICE = column(iota++, BIG_DECIMAL);
    public static final EpfTableColumn<BigDecimal> HD_PRICE = column(iota++, BIG_DECIMAL);

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EpfPricing) {
            EpfPricing other = (EpfPricing) that;
            return Objects.equal(this.get(VIDEO_ID), other.get(VIDEO_ID));
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return this.get(VIDEO_ID).hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("Pricing for %s", this.get(VIDEO_ID));
    }
}
