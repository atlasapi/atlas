package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.atlasapi.deer.elasticsearch.content.Alias;
import org.atlasapi.deer.elasticsearch.content.ContentType;
import org.atlasapi.deer.elasticsearch.content.Id;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;

public class EsContent extends EsObject {

    public final static String TOP_LEVEL_CONTAINER = "container";
    public final static String CHILD_ITEM = "child_item";
    public final static String TOP_LEVEL_ITEM = "top_item";

    public final static String ID = "id";
    public final static String TYPE = "type";
    public final static String SOURCE = "source";
    public final static String ALIASES = "aliases";
    public final static String TITLE = "title";
    public final static String FLATTENED_TITLE = "flattenedTitle";
    public final static String PARENT_TITLE = "parentTitle";
    public final static String PARENT_FLATTENED_TITLE = "parentFlattenedTitle";
    public final static String SPECIALIZATION = "specialization";
    public final static String BROADCASTS = "broadcasts";
    public final static String LOCATIONS = "locations";
    public final static String TOPICS = "topics";
    public final static String HAS_CHILDREN = "hasChildren";
    public final static String GENRE = "genre";
    public final static String PRICE = "price";
    public static final String AGE = "age";
    public static final String CONTENT_GROUPS = "contentGroups";
    public static final String PARENT = "_parent";
    public static final String PRIORITY = "priority";
    public static final String SERIES_NUMBER = "seriesNumber";
    public static final String EPISODE_NUMBER = "episodeNumber";
    public static final String BRAND = "brand";
    public static final String SERIES = "series";
    public static final String SORT_KEY = "sortKey";
    public static final String CANONICAL_ID = "canonicalId";

    public static final XContentBuilder getTopLevelMapping(String type) throws IOException {
        return addCommonProperties(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("_all")
                .field("enabled").value(false)
                .endObject()
                .startObject("properties"))
                .endObject()
                .endObject()
                .endObject();
    }

    public static final XContentBuilder getChildMapping() throws IOException {
        return addCommonProperties(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(EsContent.CHILD_ITEM)
                .startObject(PARENT)
                .field("type").value(EsContent.TOP_LEVEL_CONTAINER)
                .endObject()
                .startObject("_all")
                .field("enabled").value(false)
                .endObject()
                .startObject("_routing")
                .field("required").value(false)
                .endObject()
                .startObject("properties"))
                .endObject()
                .endObject()
                .endObject();
    }

    private static XContentBuilder addCommonProperties(XContentBuilder obj) throws IOException {
        return addScheduleOnlyProperties(obj
                .startObject(EsContent.TYPE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(ALIASES)
                .field("type").value("nested")
                .rawField("properties", EsAlias.getMapping().bytes())
                .endObject()
                .startObject(EsContent.TITLE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.FLATTENED_TITLE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.SOURCE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.SPECIALIZATION)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.TOPICS)
                .field("type").value("nested")
                .rawField("properties", EsTopicMapping.getMapping().bytes())
                .endObject()
                .startObject(EsContent.LOCATIONS)
                .field("type").value("nested")
                .rawField("properties", EsLocation.getMapping().bytes())
                .endObject()
                .startObject(EsContent.GENRE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.PRICE)
                .field("type").value("nested")
                .rawField("properties", EsPriceMapping.getMapping().bytes())
                .endObject()
                .startObject(EsContent.AGE)
                .field("type").value("integer")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.CONTENT_GROUPS)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.PRIORITY)
                .field("type").value("double")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(SERIES_NUMBER)
                .field("type").value("integer")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EPISODE_NUMBER)
                .field("type").value("integer")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(BRAND)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(SERIES)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(SORT_KEY)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(CANONICAL_ID)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
        );
    }

    private static XContentBuilder addScheduleOnlyProperties(XContentBuilder obj)
            throws IOException {
        return obj.startObject(EsContent.ID)
                .field("type").value("long")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(EsContent.BROADCASTS)
                .field("type").value("nested")
                .startObject("properties")
                .startObject(EsBroadcast.CHANNEL)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .endObject()
                .endObject();
    }

    public EsContent id(long id) {
        properties.put(ID, id);
        return this;
    }

    public EsContent type(ContentType type) {
        properties.put(TYPE, type.getKey());
        return this;
    }

    public EsContent aliases(Iterable<Alias> aliases) {
        properties.put(
                ALIASES,
                Iterables.transform(aliases, Functions.compose(TO_MAP, EsAlias.toEsAlias()))
        );
        return this;
    }

    public EsContent title(String title) {
        properties.put(TITLE, title);
        return this;
    }

    public EsContent flattenedTitle(String flattenedTitle) {
        properties.put(FLATTENED_TITLE, flattenedTitle);
        return this;
    }

    public EsContent parentTitle(String parentTitle) {
        properties.put(PARENT_TITLE, parentTitle);
        return this;
    }

    public EsContent parentFlattenedTitle(String parentFlattenedTitle) {
        properties.put(PARENT_FLATTENED_TITLE, parentFlattenedTitle);
        return this;
    }

    public EsContent source(String publisher) {
        properties.put(SOURCE, publisher);
        return this;
    }

    public EsContent specialization(String specialization) {
        properties.put(SPECIALIZATION, specialization);
        return this;
    }

    public EsContent broadcasts(Iterable<EsBroadcast> broadcasts) {
        properties.put(BROADCASTS, Iterables.transform(broadcasts, TO_MAP));
        return this;
    }

    public EsContent locations(Iterable<EsLocation> locations) {
        properties.put(LOCATIONS, Iterables.transform(locations, TO_MAP));
        return this;
    }

    public EsContent topics(Collection<EsTopicMapping> topics) {
        properties.put(TOPICS, Iterables.transform(topics, TO_MAP));
        return this;
    }

    public EsContent hasChildren(Boolean hasChildren) {
        properties.put(HAS_CHILDREN, hasChildren);
        return this;
    }

    public EsContent genre(Iterable<String> genre) {
        properties.put(GENRE, genre);
        return this;
    }

    public EsContent price(Iterable<EsPriceMapping> prices) {
        properties.put(PRICE, Iterables.transform(prices, TO_MAP));
        return this;
    }

    public EsContent age(Integer age) {
        properties.put(AGE, age);
        return this;
    }

    public EsContent contentGroups(Iterable<Id> contentGroupIds) {
        properties.put(CONTENT_GROUPS, contentGroupIds);
        return this;
    }

    public EsContent priority(Double priority) {
        properties.put(PRIORITY, priority);
        return this;
    }

    public EsContent broadcastStartTimeInMillis(Iterable<Long> startTimes) {
        properties.put(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS, startTimes);
        return this;
    }

    public EsContent brandId(Id id) {
        properties.put(BRAND, id.longValue());
        return this;
    }

    public EsContent seriesId(Id id) {
        properties.put(SERIES, id.longValue());
        return this;
    }

    public EsContent seriesNumber(Integer seriesNumber) {
        properties.put(SERIES_NUMBER, seriesNumber);
        return this;
    }

    public EsContent episodeNumber(Integer episodeNumber) {
        properties.put(EPISODE_NUMBER, episodeNumber);
        return this;
    }

    public EsContent sortKey(String sortKey) {
        properties.put(SORT_KEY, sortKey);
        return this;
    }

    public EsContent canonicalId(long id) {
        properties.put(CANONICAL_ID, id);
        return this;
    }
}
