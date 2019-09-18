package org.atlasapi.remotesite.amazon;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.metabroadcast.common.scheduling.UpdateProgress;
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
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class AmazonContentWritingItemProcessorTest {

    private AmazonContentWritingItemProcessor processor;
    private ContentExtractor extractor = new AmazonContentExtractor();
    @Mock
    private ContentResolver resolver;
    @Mock
    private ContentWriter writer;
    @Mock
    private ContentLister lister;
    @Mock
    private AmazonBrandProcessor brandProcessor;
    private OwlTelescopeReporter telescope = mock(OwlTelescopeReporter.class);
    @Captor
    private ArgumentCaptor<Episode> itemArgumentCaptor;
    @Captor
    private ArgumentCaptor<Container> containerArgumentCaptor;

    @Mock
    private AmazonTitleIndexStore amazonTitleIndexStore = mock(AmazonTitleIndexStore.class);
    @Captor
    private ArgumentCaptor<AmazonTitleIndexEntry> amazonTitleIndexEntryArgumentCaptor;

    @Before
    public void setUp() {
        when(resolver.findByCanonicalUris(anyCollection())).thenReturn(ResolvedContent.builder()
                .build());
        when(lister.listContent(any(ContentListingCriteria.class))).thenReturn(Collections.<Content>emptyIterator());
        processor = new AmazonContentWritingItemProcessor(
                extractor,
                resolver,
                writer,
                lister,
                100,
                brandProcessor,
                amazonTitleIndexStore
        );
    }

    @Test
    public void testHierarchyIsIngestedWithoutDuplicates()
            throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        TestAmazonProcessor testprocessor = new TestAmazonProcessor();
        AmazonContentHandler handler = new AmazonContentHandler(testprocessor);
        saxParser.parse(getFileAsInputStream("hierarchy.xml"), handler);

        processor.prepare(telescope);
        for (AmazonItem item : testprocessor.getItems()) {
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

    @Test
    public void testIndexIsCreated() throws ParserConfigurationException, SAXException, IOException
    {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        TestAmazonProcessor testProcessor = new TestAmazonProcessor();
        AmazonContentHandler handler = new AmazonContentHandler(testProcessor);
        saxParser.parse(getFileAsInputStream("duplicate_title.xml"), handler);

        processor.prepare(telescope);
        for (AmazonItem item : testProcessor.getItems()) {
            processor.process(item);
        }
        processor.finish();

        verify(amazonTitleIndexStore, times(2))
                .createOrUpdateIndex(amazonTitleIndexEntryArgumentCaptor.capture());
    }

    private InputStream getFileAsInputStream(String fileName) throws IOException {
        URL testFile = Resources.getResource(getClass(), fileName);
        return Resources.newInputStreamSupplier(testFile).getInput();
    }

    private class TestAmazonProcessor implements AmazonProcessor<UpdateProgress> {

        private UpdateProgress progress = UpdateProgress.START;
        private final List<AmazonItem> items = Lists.newArrayList();

        @Override
        public boolean process(AmazonItem aUItem) {
            items.add(aUItem);
            progress = progress.reduce(UpdateProgress.SUCCESS);
            return true;
        }

        @Override
        public UpdateProgress getResult() {
            return progress;
        }

        public List<AmazonItem> getItems() {
            return items;
        }
    }

}