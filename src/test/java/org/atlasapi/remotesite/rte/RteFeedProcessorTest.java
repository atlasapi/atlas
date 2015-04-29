package org.atlasapi.remotesite.rte;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.StatusReporter;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.WireFeedInput;

@RunWith(MockitoJUnitRunner.class)
public class RteFeedProcessorTest {
    private final static String FEED_PATH = "org/atlasapi/remotesite/rte/az_atom_feed.xml";
    
    private RteFeedProcessor processor;
    
    @Mock
    private ContentWriter contentWriter;
    
    @Mock
    private StatusReporter statusReporter;
    
    @Before
    public void setup() {
        processor = new RteFeedProcessor(
                contentWriter,
                new DummyContentResolver(),
                new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE),
                new RteBrandExtractor());
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testFeedProcessing() throws FileNotFoundException, IllegalArgumentException, FeedException {
        // GIVEN 
        Feed feed = feed();
        List<Entry> entries = feed.getEntries();
        ArgumentCaptor<Brand> captor = ArgumentCaptor.forClass(Brand.class);
        
        // WHEN
        processor.process(feed, statusReporter);
        
        // THEN
        verify(contentWriter, times(entries.size())).createOrUpdate(captor.capture());
        verify(statusReporter, times(entries.size())).reportStatus(anyString());
        checkWrittenBrands(entries, captor.getAllValues());
    }
    
    private void checkWrittenBrands(List<Entry> entries, List<Brand> allValues) {
        for (Entry entry: entries) {
            String canonicalUri = RteParser.canonicalUriFrom(entry.getId());
            assertThat(allValues, hasItem(createBrand(canonicalUri)));
        }
    }
    
    private Brand createBrand(String canonicalUri) {
        Brand brand = new Brand();
        brand.setCanonicalUri(canonicalUri);
        return brand;
    }

    private Feed feed() throws FileNotFoundException, IllegalArgumentException, FeedException {
        InputStream inputStream = new FileInputStream(Resources.getResource(FEED_PATH).getFile());
        Reader reader = new InputStreamReader(inputStream);
        return (Feed) new WireFeedInput().build(reader);
    }
    
    private class DummyContentResolver implements ContentResolver {
        
        @Override
        public ResolvedContent findByCanonicalUris(Iterable<? extends String> canonicalUris) {
            return new ResolvedContent(Maps.<String, Maybe<Identified>>newHashMap());
        }

        @Override
        public ResolvedContent findByUris(Iterable<String> uris) {
            throw new UnsupportedOperationException();
        }
    }
    
}
