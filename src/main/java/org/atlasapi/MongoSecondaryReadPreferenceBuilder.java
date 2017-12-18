package org.atlasapi;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.stream.MoreCollectors;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;

import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

public class MongoSecondaryReadPreferenceBuilder {

    private static final Splitter TAG_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on(":").omitEmptyStrings().trimResults(); 
    
    /**
     * Convert from a collection of strings, of format key1:value1,key2:value2
     * into a {@link DBObject} suitable for passing as a paramater to a
     * {@link ReadPreference} instantiation.
     * 
     * @param properties    Strings from which to parse tags
     * @return              A {@link ReadPreference} reflecting tag preferences
     */
    public ReadPreference fromProperties(Iterable<String> properties) {
        List<Tag> tagPreferences = StreamSupport.stream(properties.spliterator(), false)
                .flatMap(props -> PROPERTY_TO_TAG_DBO.apply(props).stream())
                .collect(MoreCollectors.toImmutableList());

        if (Iterables.isEmpty(tagPreferences)) {
            return ReadPreference.secondaryPreferred();
        }

        return ReadPreference.secondaryPreferred(new TagSet(tagPreferences));
    }

    private static Function<String, List<Tag>> PROPERTY_TO_TAG_DBO = input -> {
        ImmutableList.Builder<Tag> tags = ImmutableList.builder();

        for (String tag : TAG_SPLITTER.split(input)) {
            Iterator<String> split = KEY_VALUE_SPLITTER.split(tag).iterator();

            String key = split.next();
            String value = split.next();

            if (split.hasNext()) {
                throw new IllegalArgumentException("Invalid format for tag preference; should be key:value");
            }

            tags.add(new Tag(key, value));
        }

        return tags.build();
    };
}
