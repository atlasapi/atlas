package org.atlasapi;

import java.util.Iterator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TaggableReadPreference;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class MongoSecondaryReadPreferenceBuilderTest {

    private final MongoSecondaryReadPreferenceBuilder builder = new MongoSecondaryReadPreferenceBuilder();

    @Test
    public void testNoTags() {
        TaggableReadPreference readPreference = (TaggableReadPreference) builder.fromProperties(ImmutableSet.<String>of());

        assertTrue(readPreference.isSlaveOk());
        assertTrue(readPreference.getTagSetList().isEmpty());
    }

    @Test
    public void testWithSingleTag() {
        TaggableReadPreference readPreference = (TaggableReadPreference) builder.fromProperties(ImmutableSet.<String>of("key:value"));
        Tag tag = Iterables.getOnlyElement(readPreference.getTagSetList().get(0));
        assertTrue(readPreference.isSlaveOk());
        assertThat(tag.getValue(), is(equalTo("value")));
    }

    @Test
    public void testWithMultipleTags() {
        TaggableReadPreference readPreference = (TaggableReadPreference)
                builder.fromProperties(ImmutableSet.<String>of("key:value", "key2:value2"));

        assertTrue(readPreference.isSlaveOk());

        TagSet tagSet = readPreference.getTagSetList().get(0);
        Iterator<Tag> it = tagSet.iterator();

        Tag tag = it.next();
        assertThat(tag.getValue(), is(equalTo("value")));

        tag = it.next();
        assertThat(tag.getValue(), is(equalTo("value2")));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidTags() {
        builder.fromProperties(ImmutableSet.<String>of("key:value:other"));
    }
}
