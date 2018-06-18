package org.atlasapi.remotesite.amazonunbox;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

            String tmpXmlFilename = "tmpAmazonCatalogue.tmp";

            //Read the file, remove invalid xml, and store it as a tmp file.
            BufferedWriter writer = new BufferedWriter(new FileWriter(tmpXmlFilename));
            BufferedReader reader = new BufferedReader(new InputStreamReader(zis));
            while (reader.ready()){
                String line = reader.readLine();
                String clean = line.replaceAll(xml10pattern, "");
                if(!line.equals(clean)){
                    log.warn("Removed illegal xml from line: "+line);
                }
                writer.write(clean);
            }
            InputStream fis = new FileInputStream(tmpXmlFilename);

            saxParser.parse(fis, handler);
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
