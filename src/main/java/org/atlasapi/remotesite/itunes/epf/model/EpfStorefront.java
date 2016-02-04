package org.atlasapi.remotesite.itunes.epf.model;

import java.util.List;

import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.INTEGER;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.STRING;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.TIMESTAMP;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.column;

public class EpfStorefront extends EpfTableRow {

    public static Function<List<String>, EpfStorefront> FROM_ROW_PARTS = new Function<List<String>, EpfStorefront>() {
        @Override
        public EpfStorefront apply(List<String> input) {
            return new EpfStorefront(input);
        }
    };

    private static int iota = 0;
    public static final EpfTableColumn<Timestamp> EXPORT_DATE = column(iota++, TIMESTAMP);
    public static final EpfTableColumn<Integer> STOREFRONT_ID = column(iota++, INTEGER);
    public static final EpfTableColumn<String> COUNTRY_CODE = column(iota++, STRING);
    public static final EpfTableColumn<String> NAME = column(iota++, STRING);

    public EpfStorefront(List<String> rowParts) {
        super(rowParts);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EpfCollection) {
            EpfCollection other = (EpfCollection) that;
            return Objects.equal(this.get(STOREFRONT_ID), other.get(STOREFRONT_ID));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.get(STOREFRONT_ID);
    }

    @Override
    public String toString() {
        return String.format("Country %s", this.get(STOREFRONT_ID));
    }


}
