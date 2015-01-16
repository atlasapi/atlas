package org.atlasapi.remotesite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.DateTimeZones;


public class ContentMergerTest {
    
    @Test
    public void testVersionMerger() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP);
        
        Item current = new Item();
        Item extracted = new Item();
        
        Broadcast broadcast1 = new Broadcast("http://example.com/channel1", 
                new DateTime(DateTimeZones.UTC),
                new DateTime(DateTimeZones.UTC).plusHours(1));
        Broadcast broadcast2 = new Broadcast("http://example.com/channel1", 
                new DateTime(DateTimeZones.UTC).plusHours(4),
                new DateTime(DateTimeZones.UTC).plusHours(5));
        Version version1 = new Version();
        version1.setCanonicalUri("http://example.org/1");
        version1.setBroadcasts(ImmutableSet.of(broadcast1));
        current.setVersions(ImmutableSet.of(version1));
        Version version2 = new Version();
        version2.setCanonicalUri("http://example.org/1");
        version2.setBroadcasts(ImmutableSet.of(broadcast2));
        extracted.setVersions(ImmutableSet.of(version2));
        Item merged = contentMerger.merge(current, extracted);
        
        assertEquals(2, Iterables.getOnlyElement(merged.getVersions()).getBroadcasts().size());
    }
    
    @Test
    public void testVersionMergeReplaceStrategy() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP);
        
        Item current = new Item();
        Item extracted = new Item();
        
        Version version1 = new Version();
        version1.setCanonicalUri("http://example.org/1");
        current.setVersions(ImmutableSet.of(version1));

        Restriction restriction = new Restriction();
        restriction.setRestricted(true);

        Version version2 = new Version();
        version2.setCanonicalUri("http://example.org/2");
        version2.setRestriction(restriction);
        
        extracted.setVersions(ImmutableSet.of(version2));
        Item merged = contentMerger.merge(current, extracted);

        Version mergedVersion = Iterables.getOnlyElement(merged.getVersions());
        assertEquals("http://example.org/2", mergedVersion.getCanonicalUri());
        assertTrue(mergedVersion.getRestriction().isRestricted());
    }

    @Test
    public void testTopicMergerOnSuppliedEquivalence() {
        final ContentMerger contentMerger = new ContentMerger(MergeStrategy.KEEP, MergeStrategy.replaceTopicsBasedOn(new Equivalence<TopicRef>() {
            @Override
            protected boolean doEquivalent(TopicRef a, TopicRef b) {
                return Objects.equal(a.getOffset(), b.getOffset());
            }

            @Override
            protected int doHash(TopicRef topicRef) {
                return Objects.hashCode(topicRef.getOffset());
            }
        }));

        TopicRef a1 = new TopicRef(9000L, 0f, false, TopicRef.Relationship.ABOUT, 45);
        TopicRef a2 = new TopicRef(9001L, 0f, true, TopicRef.Relationship.TRANSCRIPTION, 45);
        TopicRef b1 = new TopicRef(9000L, 0f, false, TopicRef.Relationship.ABOUT, 450);
        TopicRef b2 = new TopicRef(9001L, 0f, true, TopicRef.Relationship.TRANSCRIPTION, 450);
        TopicRef c1 = new TopicRef(9000L, 0f, false, TopicRef.Relationship.ABOUT, 324324);
        TopicRef d2 = new TopicRef(9001L, 0f, true, TopicRef.Relationship.TRANSCRIPTION, 234098);
        TopicRef n1 = new TopicRef(201L, 0f, true, TopicRef.Relationship.ABOUT);
        TopicRef n2 = new TopicRef(9001L, 0f, true, TopicRef.Relationship.ABOUT);

        Item current = new Item();
        current.setTopicRefs(ImmutableList.of(a1, b1, c1, n1));

        Item extracted = new Item();
        extracted.setTopicRefs(ImmutableList.of(a2, b2, d2, n2));

        Item merged = contentMerger.merge(current, extracted);
        List<TopicRef> mergedRefs = merged.getTopicRefs();
        assertEquals(5, mergedRefs.size());
        assertTrue(mergedRefs.contains(a2));
        assertTrue(mergedRefs.contains(b2));
        assertTrue(mergedRefs.contains(c1));
        assertTrue(mergedRefs.contains(d2));
        assertTrue(mergedRefs.contains(n2));
    }

}
