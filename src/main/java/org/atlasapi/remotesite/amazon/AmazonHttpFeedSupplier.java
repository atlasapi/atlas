package org.atlasapi.remotesite.amazon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * TODO: This needs refactoring to avoid the building of the 
 *       entire resultset in memory. Would like to change to an Iterable, which
 *       will involve downloading the file first, then parsing the response.
 *       Otherwise we may timeout on http connections if the processing of the
 *       file takes a while
 *
 */
public class AmazonHttpFeedSupplier implements Supplier<ImmutableList<AmazonItem>> {

    private static final Logger log = LoggerFactory.getLogger(AmazonHttpFeedSupplier.class);

    private static final String TMP_FILENAME = "tmpAmazonCatalogue.tmp";
    private static final String REPLACEMENT_STRING = "[?]";

    private final String uri;
    private Pattern encodePoints = Pattern.compile("(&#[0-9]+;)");
    private DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
    private static final String OPENING_TAG = "<test>";
    private static final String CLOSING_TAG = "</test>";

    public AmazonHttpFeedSupplier(String uri) {
        this.uri = checkNotNull(uri);
    }
    
    @Override
    public ImmutableList<AmazonItem> get() {
        HttpGet get = new HttpGet(uri);
        final ItemCollatingAmazonProcessor processor = new ItemCollatingAmazonProcessor();
        final AmazonContentHandler handler = new AmazonContentHandler(processor);

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

            //Read the file, remove invalid xml, and store it as a tmp file.
            createTmpFileWithCleanXml(zis);

            InputStream fis = new FileInputStream(TMP_FILENAME);
            saxParser.parse(fis, handler);
            zis.close();

            return processor.getResult();

        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTmpFileWithCleanXml(ZipInputStream zis)
            throws IOException, ParserConfigurationException {
        log.info("Cleaning invalid xml characters from Amazon's catalogue. "
                 + "The tmp file will be stored at {}", TMP_FILENAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(zis));
        BufferedWriter writer = new BufferedWriter(new FileWriter(TMP_FILENAME));
        while (reader.ready()){
            String line = reader.readLine();
            writer.write(cleanLine(line));
            writer.newLine();
        }
        writer.close();
    }

    //Try to parse encoded characters, e.g. &#55357; . Replace the ones that are invalid ones.
    private String cleanLine(String line) throws ParserConfigurationException, IOException {
        String outcome = line;
        boolean retry;
        do {
            Matcher matcher = encodePoints.matcher(outcome);
            retry = false;
            while (matcher.find()) {
                String encodePoint = matcher.group(1);
                try {
                    parseEncodedCharacter(encodePoint);
                } catch (SAXException e) {
                    log.warn(
                            "Replaced illegal XML character {} from {} with a {}",
                            encodePoint, line, REPLACEMENT_STRING);
                    outcome = outcome.replaceAll(encodePoint, REPLACEMENT_STRING);
                    retry = true;
                }
            }
        } while (retry);
        return outcome;
    }

    public void parseEncodedCharacter(String xml) throws
            SAXException, ParserConfigurationException, IOException {
        DocumentBuilder builder = documentFactory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(OPENING_TAG + xml + CLOSING_TAG));
        builder.parse(is);
    }

    private static class ItemCollatingAmazonProcessor implements
            AmazonProcessor<ImmutableList<AmazonItem>> {
    
        private final ImmutableList.Builder<AmazonItem> items = ImmutableList.builder();
        
        @Override
        public boolean process(AmazonItem item) {
            items.add(item);
            return true;
        }
    
        @Override
        public ImmutableList<AmazonItem> getResult() {
            return items.build();
        };
    }
    
}
