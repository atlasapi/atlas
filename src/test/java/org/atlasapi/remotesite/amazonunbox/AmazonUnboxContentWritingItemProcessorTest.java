package org.atlasapi.remotesite.amazonunbox;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentExtractor;

import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xml.sax.SAXException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AmazonUnboxContentWritingItemProcessorTest {

    private AmazonUnboxContentWritingItemProcessor processor;
    private ContentExtractor extractor = new AmazonUnboxContentExtractor();
    @Mock
    private ContentResolver resolver;
    @Mock
    private ContentWriter writer;
    @Mock
    private ContentLister lister;
    @Mock
    private AmazonUnboxBrandProcessor brandProcessor;
    @Captor
    private ArgumentCaptor<Episode> itemArgumentCaptor;
    @Captor
    private ArgumentCaptor<Container> containerArgumentCaptor;

    @Before
    public void setUp() {
        when(resolver.findByCanonicalUris(anyCollection())).thenReturn(ResolvedContent.builder()
                .build());
        when(lister.listContent(any(ContentListingCriteria.class))).thenReturn(Collections.<Content>emptyIterator());
        processor = new AmazonUnboxContentWritingItemProcessor(
                extractor,
                resolver,
                writer,
                lister,
                100,
                brandProcessor
        );
    }

    @Test
    public void testHierarchyIsIngestedWithoutDuplicates()
            throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        TestAmazonUnboxProcessor testprocessor = new TestAmazonUnboxProcessor();
        AmazonUnboxContentHandler handler = new AmazonUnboxContentHandler(testprocessor);
        saxParser.parse(getFileAsInputStream("hierarchy.xml"), handler);

        for (AmazonUnboxItem item : testprocessor.getItems()) {
            processor.process(item);
        }
        processor.finish();
        verify(writer, times(3)).createOrUpdate(itemArgumentCaptor.capture());
        verify(writer, times(3)).createOrUpdate(containerArgumentCaptor.capture());
        List<Container> allValues = containerArgumentCaptor.getAllValues();
        int series = 0;
        int brands = 0;
        for (Container value : allValues) {
            if (value instanceof Brand) {
                brands++;
            } else if (value instanceof Series) {
                series++;
            }
        }
        assertEquals(series, 2);
        assertEquals(brands, 1);
    }

    private InputStream getFileAsInputStream(String fileName) throws IOException {
        URL testFile = Resources.getResource(getClass(), fileName);
        return Resources.newInputStreamSupplier(testFile).getInput();
    }

    private class TestAmazonUnboxProcessor implements AmazonUnboxProcessor<UpdateProgress> {

        private UpdateProgress progress = UpdateProgress.START;
        private final List<AmazonUnboxItem> items = Lists.newArrayList();

        @Override
        public boolean process(AmazonUnboxItem aUItem) {
            items.add(aUItem);
            progress = progress.reduce(UpdateProgress.SUCCESS);
            return true;
        }

        @Override
        public UpdateProgress getResult() {
            return progress;
        }

        public List<AmazonUnboxItem> getItems() {
            return items;
        }
    }

}