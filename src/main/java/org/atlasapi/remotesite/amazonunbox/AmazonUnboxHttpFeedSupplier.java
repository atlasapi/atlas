package org.atlasapi.remotesite.amazonunbox;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.metabroadcast.common.http.HttpStatusCode;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

    private final String uri;

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

            String beginning = "<?xml version=\"1.0\" encoding=\"utf-16\"?>";
            List<InputStream> streams = Arrays.asList(
                    new ByteArrayInputStream(beginning.getBytes()), zis);
            InputStream is = new SequenceInputStream(Collections.enumeration(streams));

            saxParser.parse(is, handler);
            zis.close();

            return processor.getResult();

        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
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
