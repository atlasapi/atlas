package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Function;
import org.atlasapi.deer.elasticsearch.content.Alias;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsAlias extends EsObject {

    private static final Function<Alias, EsAlias> TO_ALIAS = EsAlias::valueOf;
    public static final String NAMESPACE = "namespace";
    public static final String VALUE = "value";

    public static final XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(EsAlias.NAMESPACE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsAlias.VALUE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .endObject();
    }

    public static final Function<Alias, EsAlias> toEsAlias() {
        return TO_ALIAS;
    }

    public static final EsAlias valueOf(Alias alias) {
        checkNotNull(alias);
        return new EsAlias()
                .namespace(alias.getNamespace())
                .value(alias.getValue());
    }

    public static EsAlias fromMap(Map<String, Object> map) {
        return new EsAlias()
                .namespace((String)map.get(NAMESPACE))
                .value((String)map.get(VALUE));
    }

    public EsAlias namespace(String namespace) {
        properties.put(NAMESPACE, namespace);
        return this;
    }

    public EsAlias value(String value) {
        properties.put(VALUE, value);
        return this;
    }

}
