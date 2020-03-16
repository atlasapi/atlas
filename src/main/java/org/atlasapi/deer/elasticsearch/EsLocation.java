package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.atlasapi.deer.elasticsearch.content.Alias;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EsLocation extends EsObject {

    public static final String AVAILABILITY_TIME = "availabilityTime";
    public static final String AVAILABILITY_END_TIME = "availabilityEndTime";
    public static final String ALIASES = "aliases";

    public static XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(AVAILABILITY_TIME)
                .field("type").value("date")
                .field("format").value("dateOptionalTime")
                .endObject()
                .startObject(AVAILABILITY_END_TIME)
                .field("type").value("date")
                .field("format").value("dateOptionalTime")
                .endObject()
                .startObject(ALIASES)
                .field("type").value("nested")
                .rawField("properties", EsAlias.getMapping().bytes())
                .endObject()
                .endObject();
    }

    public EsLocation availabilityTime(Date availabilityTime) {
        properties.put(AVAILABILITY_TIME, availabilityTime);
        return this;
    }

    public EsLocation availabilityEndTime(Date availabilityEndTime) {
        properties.put(AVAILABILITY_END_TIME, availabilityEndTime);
        return this;
    }

    public EsLocation aliases(Iterable<Alias> aliases) {
        properties.put(
                ALIASES,
                Iterables.transform(aliases, Functions.compose(TO_MAP, EsAlias.toEsAlias()))
        );
        return this;
    }

    public EsLocation aliasesFromEs(Iterable<EsAlias> aliases) {
        properties.put(
                ALIASES,
                StreamSupport.stream(aliases.spliterator(), false)
                        .map(TO_MAP::apply)
                        .collect(Collectors.toList())
        );

        return this;
    }

    public static EsLocation fromMap(Map<String, Object> map) {
        EsLocation esLocation = new EsLocation();
        if (map.get(AVAILABILITY_TIME) != null) {
            esLocation.availabilityTime(new DateTime(map.get(AVAILABILITY_TIME)).toDate());
        }
        if (map.get(AVAILABILITY_END_TIME) != null) {
            esLocation.availabilityEndTime(new DateTime(map.get(AVAILABILITY_END_TIME)).toDate());
        }

        if (map.get(ALIASES) != null) {
                esLocation.aliasesFromEs(
                        (List<EsAlias>) StreamSupport.stream(
                                ((Iterable<Map>)map.get(ALIASES)).spliterator(),
                                false
                        )
                                .map(EsAlias::fromMap)
                                .collect(Collectors.toList())
                );
        }
        return esLocation;
    }

}
