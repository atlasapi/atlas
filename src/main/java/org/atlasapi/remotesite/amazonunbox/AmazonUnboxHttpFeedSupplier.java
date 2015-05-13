package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpRequest;


/**
 * TODO: This needs refactoring to avoid the building of the 
 *       entire resultset in memory. Would like to change to an Iterable, which
 *       will involve downloading the file first, then parsing the response.
 *       Otherwise we may timeout on http connections if the processing of the
 *       file takes a while
 *
 */
public class AmazonUnboxHttpFeedSupplier implements Supplier<ImmutableList<AmazonUnboxItem>> {

    private final SimpleHttpClient httpClient;
    private final String uri;

    public AmazonUnboxHttpFeedSupplier(SimpleHttpClient httpClient, String uri) {
        this.uri = checkNotNull(uri);
        this.httpClient = checkNotNull(httpClient);
    }
    
    @Override
    public ImmutableList<AmazonUnboxItem> get() {
        
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            final SAXParser saxParser = factory.newSAXParser();
            final ItemCollatingAmazonUnboxProcessor processor = new ItemCollatingAmazonUnboxProcessor();
            final AmazonUnboxContentHandler handler = new AmazonUnboxContentHandler(processor);
            
            return httpClient.get(new SimpleHttpRequest<>(uri, new HttpResponseTransformer<ImmutableList<AmazonUnboxItem>>() {
    
                @Override
                public ImmutableList<AmazonUnboxItem> transform(HttpResponsePrologue prologue, InputStream body)
                        throws HttpException, Exception {
                    if (HttpStatusCode.OK.code() != prologue.statusCode()) {
                        throw new RuntimeException("Response code " + prologue.statusCode() + " returned from " + uri);
                    }
                    
                    saxParser.parse(body, handler);
                    return processor.getResult();
                }
            }));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        
    }

    private static class ItemCollatingAmazonUnboxProcessor implements AmazonUnboxProcessor<ImmutableList<AmazonUnboxItem>> {
    
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
