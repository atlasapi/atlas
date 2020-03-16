package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Function;

import java.util.HashMap;
import java.util.Map;

public class EsObject {

    public final static FromEsObjectToMap TO_MAP = new FromEsObjectToMap();

    protected Map<String, Object> properties = new HashMap<String, Object>() {

        @Override
        public Object put(String k, Object v) {
            if (k != null && v != null) {
                return super.put(k, v);
            } else {
                return null;
            }
        }
    };

    public Map<String, Object> toMap() {
        return properties;
    }

    public static class FromEsObjectToMap implements Function<EsObject, Map<String, Object>> {

        @Override
        public Map<String, Object> apply(EsObject input) {
            return input.toMap();
        }
    }
}
