package org.atlasapi.remotesite.rte;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.ParsingException;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.StatusReporter;

@RunWith(MockitoJUnitRunner.class)
public class RteFeedProcessorTest {
    private static final String STALE_BRAND_URI = "http://example.org/stale";

    private final static String FEED_PATH = "org/atlasapi/remotesite/rte/search_feed.xml";
    
    private RteFeedProcessor processor;
    
    @Mock
    private ContentWriter contentWriter;
    
    @Mock
    private StatusReporter statusReporter;
    
    @Mock
    private ContentLister contentLister;
    
    @Before
    public void setup() {
        processor = new RteFeedProcessor(
                contentWriter,
                new DummyContentResolver(),
                new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE),
                contentLister, 
                new RteBrandExtractor());
    }
    
    @Test
    public void testFeedProcessing() throws IllegalArgumentException, ParsingException, IOException {
        // GIVEN 
        Brand staleBrand = createBrand(STALE_BRAND_URI);
        staleBrand.setRelatedLinks(
                ImmutableSet.of(RelatedLink.relatedLink(LinkType.VOD, "http://example.org").build()));
        
        Document document = feed();
        ArgumentCaptor<Brand> captor = ArgumentCaptor.forClass(Brand.class);
        
        when(contentLister.listContent(
                new ContentListingCriteria.Builder()
                                          .forPublisher(Publisher.RTE)
                                          .build())
            ).thenReturn(ImmutableSet.<Content>of(staleBrand).iterator());
        
        // WHEN
        processor.process(document, statusReporter);
        
        // THEN
        verify(contentWriter, times(3)).createOrUpdate(captor.capture());
        verify(statusReporter, times(2)).reportStatus(anyString());
        checkWrittenBrands(document, captor.getAllValues());
    }
    
    private void checkWrittenBrands(Document doc, List<Brand> allValues) {
        Elements docs = doc.getRootElement()
                           .getFirstChildElement("result")
                           .getChildElements();

        // Expect one write for each brand in the feed, plus a write of the 
        // stale brand
        assertThat(allValues.size(), is(docs.size() + 1));
        ImmutableMap<String, Brand> uriToContentMap = Maps.uniqueIndex(allValues, Content.TO_URI);
        
        for (int i = 0; i < docs.size(); i++) {
            
            String canonicalUri = RteParser.canonicalUriFrom(identifierElementFrom(docs.get(i)));
            assertTrue(uriToContentMap.containsKey(canonicalUri));
        }
        
        // The stale brand, not present in the feed, should have its
        // related links removed
        assertTrue(uriToContentMap.get(STALE_BRAND_URI).getRelatedLinks().isEmpty());
    }
    
    private String identifierElementFrom(Element element) {
        Elements childElements = element.getChildElements();
        for (int i = 0; i < childElements.size(); i++) {
            Element childElement = childElements.get(i);
            String attrName = childElement.getAttribute("name").getValue();
            String value = childElement.getValue();
            if (attrName.equals("identifier")) {
                return value;
            }
        }
        throw new IllegalArgumentException("Failed to find identifier element");
    }
    
    private Brand createBrand(String canonicalUri) {
        Brand brand = new Brand();
        brand.setCanonicalUri(canonicalUri);
        return brand;
    }

    private Document feed() throws IllegalArgumentException, ParsingException, IOException {
        Builder parser = new Builder();
        return parser.build(new FileInputStream(Resources.getResource(FEED_PATH).getFile()))
                     .getDocument();
        
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
