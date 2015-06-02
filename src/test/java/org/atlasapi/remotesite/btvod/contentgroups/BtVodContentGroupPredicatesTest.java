package org.atlasapi.remotesite.btvod.contentgroups;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.btvod.BtMpxVodClient;
import org.atlasapi.remotesite.btvod.BtVodContentGroupPredicate;
import org.atlasapi.remotesite.btvod.VodEntryAndContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

@RunWith( MockitoJUnitRunner.class )
public class BtVodContentGroupPredicatesTest {

    private static final String FEED_NAME = "test";
    private static final String ID_IN_FEED = "1";
    
    @Mock
    private BtMpxVodClient mpxClient;
    
    @Test
    public void testMpxContentGroupPredicate() throws IOException {
        when(mpxClient.getFeed(FEED_NAME))
            .thenReturn(ImmutableSet.of(entryWithId(ID_IN_FEED)).iterator());
        
        BtVodContentGroupPredicate predicate = BtVodContentGroupPredicates.mpxContentGroupPredicate(mpxClient, FEED_NAME);
        predicate.init();
        
        assertTrue(predicate.apply(vodEntryAndContent(ID_IN_FEED)));
        assertFalse(predicate.apply(vodEntryAndContent("3")));
        
    }
    
    private VodEntryAndContent vodEntryAndContent(String id) {
        return new VodEntryAndContent(entryWithId(id), new Item());
    }
    
    private BtVodEntry entryWithId(String id) {
        BtVodEntry entry = new BtVodEntry();
        entry.setId(id);
        return entry;
    }
}
