package org.atlasapi.remotesite.amazonunbox;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.remotesite.pa.channels.PaChannelGroupsIngester;

import com.metabroadcast.common.http.HttpStatusCode;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * TODO: This needs refactoring to avoid the building of the 
 *       entire resultset in memory. Would like to change to an Iterable, which
 *       will involve downloading the file first, then parsing the response.
 *       Otherwise we may timeout on http connections if the processing of the
 *       file takes a while
 *
 */
public class AmazonUnboxHttpFeedSupplier implements Supplier<ImmutableList<AmazonUnboxItem>> {

    private static final Logger log = LoggerFactory.getLogger(AmazonUnboxHttpFeedSupplier.class);

    private final String uri;
    String xml10pattern = "[^"
                          + "\u0009\r\n"
                          + "\u0020-\uD7FF"
                          + "\uE000-\uFFFD"
                          + "\ud800\udc00-\udbff\udfff"
                          + "]";

    public AmazonUnboxHttpFeedSupplier(String uri) {
        this.uri = checkNotNull(uri);
    }
    
    @Override
    public ImmutableList<AmazonUnboxItem> get() {
        HttpGet get = new HttpGet(uri);
        final ItemCollatingAmazonUnboxProcessor processor = new ItemCollatingAmazonUnboxProcessor();
        final AmazonUnboxContentHandler handler = new AmazonUnboxContentHandler(processor);

        try (
                CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(get)
        ) {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            final SAXParser saxParser = factory.newSAXParser();

            int statusCode = response.getStatusLine().getStatusCode();
            if (HttpStatusCode.OK.code() != statusCode) {
                throw new RuntimeException("Response code " + statusCode + " returned from " + uri);
            }

            ZipInputStream zis = new ZipInputStream(response.getEntity().getContent());
            zis.getNextEntry();

            log.info("Attempting to load the whole amazon catalogue in memory.");
            StringWriter writer = new StringWriter();
            IOUtils.copy(zis, writer, "UTF-8");
            String theString = writer.toString();
            log.info("Succeeded loading amazon catalogue in memory. Attempting to replace illegal characters.");
            String theCleanedString = theString.replaceAll(xml10pattern, "");
            log.info("Succeeded replacing illegal characters.");

            //  FilteringInputStream fis = new FilteringInputStream(zis);

            saxParser.parse(theCleanedString, handler);
            zis.close();

            return processor.getResult();

        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    private class FilteringInputStream extends InputStream{

        InputStream delegate;

        public FilteringInputStream(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }
    }

    private static class ItemCollatingAmazonUnboxProcessor implements
            AmazonUnboxProcessor<ImmutableList<AmazonUnboxItem>> {
    
        private final ImmutableList.Builder<AmazonUnboxItem> items = ImmutableList.builder();
        
        @Override
        public boolean process(AmazonUnboxItem item) {
            items.add(item);
            return true;
        }
    
        @Override
        public ImmutableList<AmazonUnboxItem> getResult() {
            return items.build();
        };
    }
    
}
