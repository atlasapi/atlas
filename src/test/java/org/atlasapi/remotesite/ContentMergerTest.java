package org.atlasapi.remotesite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentMerger.AliasMergeStrategy;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ContentMergerTest {
    
    private static final Publisher PUBLISHER = Publisher.METABROADCAST;

    @Test
    public void testVersionMerger() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
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
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
        Series current = new Series();
        Series extracted = new Series();
        
        Version version1 = new Version();
        version1.setCanonicalUri("http://example.org/1");
        current.setVersions(ImmutableSet.of(version1));

        Restriction restriction = new Restriction();
        restriction.setRestricted(true);

        Version version2 = new Version();
        version2.setCanonicalUri("http://example.org/2");
        version2.setRestriction(restriction);
        
        extracted.setVersions(ImmutableSet.of(version2));
        Series merged = (Series)contentMerger.merge(current, extracted);

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
        }), MergeStrategy.REPLACE);

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

    @Test
    public void testItemItemMergingProducesItem() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
        Item current = createItem("old title", PUBLISHER);
        Item extracted = createItem("new title", PUBLISHER);
        
        Item merged = contentMerger.merge(current, extracted);
        
        assertTrue("Merged object should be of same type as extracted object", !(merged instanceof Episode));
        assertEquals("new title", merged.getTitle());
    }
    
    @Test
    public void testEpisodeItemMergingProducesItem() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
        Episode current = createEpisode("old title", PUBLISHER, 3);
        Item extracted = createItem("new title", PUBLISHER);
        
        Item merged = contentMerger.merge(current, extracted);
        
        assertTrue("Merged object should be of same type as extracted object", !(merged instanceof Episode));
        assertEquals("new title", merged.getTitle());
    }
    
    @Test
    public void testItemEpisodeMergingProducesEpisode() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
        String extractedTitle = "new title";
        Integer extractedEpisodeNum = 5;
        
        Item current = createItem("old title", PUBLISHER);
        Episode extracted = createEpisode(extractedTitle, PUBLISHER, extractedEpisodeNum);
        
        Episode merged = (Episode) contentMerger.merge(current, extracted);
        
        assertEquals(extractedTitle, merged.getTitle());
        assertEquals(extractedEpisodeNum, merged.getEpisodeNumber());
    }
    
    @Test
    public void testEpisodeEpisodeMergingProducesEpisode() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
        String extractedTitle = "new title";
        Integer extractedEpisodeNum = 5;
        
        Episode current = createEpisode("old title", PUBLISHER, 3);
        Episode extracted = createEpisode(extractedTitle, PUBLISHER, extractedEpisodeNum);
        
        Episode merged = (Episode) contentMerger.merge(current, extracted);
        
        assertEquals(extractedTitle, merged.getTitle());
        assertEquals(extractedEpisodeNum, merged.getEpisodeNumber());
    }
    
    @Test
    public void testAliasMergeStrategyInvoked() {
        AliasMergeStrategy aliasMergeStrategy = mock(AliasMergeStrategy.class);
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, aliasMergeStrategy);
        
        Item current = createItem("title", Publisher.METABROADCAST);
        Item extracted = createItem("title", Publisher.METABROADCAST);
        when(aliasMergeStrategy.mergeAliases(current, extracted)).thenReturn(current);
        
        contentMerger.merge(current, extracted);
        verify(aliasMergeStrategy).mergeAliases(current, extracted);
    }
    
    @Test
    public void testMergeAliasesMergeStrategy() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.MERGE);
        Item current = createItem("title", Publisher.METABROADCAST);
        Item extracted = createItem("title", Publisher.METABROADCAST);
        
        Set<Alias> currentAliases = ImmutableSet.of(new Alias("1", "2"), new Alias("2", "3"));
        Set<Alias> extractedAliases = ImmutableSet.of(new Alias("3", "4"));
        current.setAliases(currentAliases);
        extracted.setAliases(extractedAliases);
        
        current.setAliasUrls(ImmutableSet.of("http://a.com/b", "http://b.com/c"));
        extracted.setAliasUrls(ImmutableSet.of("http://c.com/d"));
        
        Item merged = contentMerger.merge(current, extracted);
        
        assertEquals(Sets.union(currentAliases, extractedAliases), merged.getAliases());
    }
    
    @Test
    public void testReplaceAliasesMergeStrategy() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        Item current = createItem("title", Publisher.METABROADCAST);
        Item extracted = createItem("title", Publisher.METABROADCAST);
        Set<Alias> extractedAliases = ImmutableSet.of(new Alias("3", "4"));
        
        current.setAliases(ImmutableSet.of(new Alias("1", "2"), new Alias("2", "3")));
        extracted.setAliases(extractedAliases);
        
        current.setAliasUrls(ImmutableSet.of("http://a.com/b", "http://b.com/c"));
        extracted.setAliasUrls(ImmutableSet.of("http://c.com/d"));
        
        Item merged = contentMerger.merge(current, extracted);
        
        assertEquals(extractedAliases, merged.getAliases());
    }
    
    @Test
      public void testKeepAliasesMergeStrategy() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.KEEP);
        Item current = createItem("title", Publisher.METABROADCAST);
        Item extracted = createItem("title", Publisher.METABROADCAST);
        Set<Alias> currentAliases = ImmutableSet.of(new Alias("1", "2"), new Alias("2", "3"));
        current.setAliases(currentAliases);
        extracted.setAliases(ImmutableSet.of(new Alias("3", "4")));

        current.setAliasUrls(ImmutableSet.of("http://a.com/b", "http://b.com/c"));
        extracted.setAliasUrls(ImmutableSet.of("http://c.com/d"));

        Item merged = contentMerger.merge(current, extracted);

        assertEquals(currentAliases, merged.getAliases());
    }


    @Test
    public void testMergeTopicsMergeStrategy() {
        ContentMerger contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.MERGE, MergeStrategy.KEEP);
        Item current = createItem("title", Publisher.METABROADCAST);
        Item extracted = createItem("title", Publisher.METABROADCAST);

        TopicRef topicRef1 = mock(TopicRef.class);
        TopicRef topicRef2 = mock(TopicRef.class);
        TopicRef topicRef3 = mock(TopicRef.class);

        current.addTopicRef(topicRef1);
        current.addTopicRef(topicRef2);

        extracted.addTopicRef(topicRef3);

        Item merged = contentMerger.merge(current, extracted);

        assertEquals(
                ImmutableSet.copyOf(merged.getTopicRefs()),
                ImmutableSet.of(topicRef1, topicRef2, topicRef3)
        );

    }

    private Item createItem(String title, Publisher publisher) {
        Item item = new Item("item", "curie", publisher);
        
        item.setTitle(title);
        
        return item;
    }

    private Episode createEpisode(String title, Publisher publisher, Integer episodeNum) {
        Episode ep = new Episode("episode", "curie", publisher);
        
        ep.setTitle(title);
        ep.setEpisodeNumber(episodeNum);
        
        return ep;
    }
}
