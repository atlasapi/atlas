package org.atlasapi.deer.elasticsearch;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Currency;

public class EsPriceMapping extends EsObject {

    public static final String CURRENCY = "currency";
    public static final String VALUE = "value";

    public static final XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(EsPriceMapping.CURRENCY)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsPriceMapping.VALUE)
                .field("type").value("integer")
                .field("index").value("not_analyzed")
                .endObject()
                .endObject();
    }

    public EsPriceMapping currency(Currency currency) {
        properties.put(CURRENCY, currency.getCurrencyCode());
        return this;
    }

    public EsPriceMapping value(Integer value) {
        properties.put(VALUE, value);
        return this;
    }

}
