package org.atlasapi.remotesite.btvod.portal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.http.HttpStatusCodeException;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpRequest;


public class XmlPortalClient implements PortalClient {

    private static final int MAX_PAGES_BEFORE_BAILING = 1000;

    private static final Logger log = LoggerFactory.getLogger(XmlPortalClient.class);
    
    private final String uriBase;
    private final SimpleHttpClient httpClient;
    private final Builder parser = new Builder();

    public XmlPortalClient(String uriBase, SimpleHttpClient httpClient) {
        this.httpClient = checkNotNull(httpClient);
        this.uriBase = checkNotNull(uriBase);
    }
    
    public Optional<Set<String>> getProductIdsForGroup(String groupId) {
        int pageNumber = 1;
        PageData pageData = null;
        ImmutableSet.Builder<String> ids = ImmutableSet.builder();
        do {
            try {
                pageData = getProductIdsForGroupPage(groupId, pageNumber);
            } catch (HttpStatusCodeException e) {
                if (HttpStatusCode.NOT_FOUND.is(e.getStatusCode())) {
                    return Optional.absent();
                }
                throw Throwables.propagate(e);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            ids.addAll(pageData.ids);
            pageNumber++;
            if (pageNumber > MAX_PAGES_BEFORE_BAILING) {
                throw new IllegalStateException("Too many pages: are we in an infinite loop?");
            }
        } while (pageData.end < pageData.total);
                
        return Optional.of((Set<String>) ids.build());
    }
    
    private PageData getProductIdsForGroupPage(String groupId, int pageNumber) throws HttpException, Exception {
        Document document = 
            httpClient.get(
                    new SimpleHttpRequest<Document>(uriBase + groupId + "/" + String.valueOf(pageNumber) + ".xml", 
                    TRANSFORMER)
                );
        return parsePage(document.getRootElement());        
    }
    
    private final HttpResponseTransformer<Document> TRANSFORMER = new HttpResponseTransformer<Document>() {
        @Override
        public Document transform(HttpResponsePrologue response, InputStream in) throws HttpException, IOException {
            if (!HttpStatusCode.OK.is(response.statusCode())) {
                InputStreamReader reader = new InputStreamReader(in);
                String responseBody = CharStreams.toString(reader);
                
                throw new HttpStatusCodeException(response, responseBody);
            }
                
            try {
                return parser.build(in);
            } catch (Exception e) {
                log.error("Exception when processing shows document",e);
                throw Throwables.propagate(e);
            }
        }
    };
    
    private PageData parsePage(Element rootElement) {
        ImmutableSet.Builder<String> ids = ImmutableSet.builder();
        Elements children = rootElement.getChildElements();
        for(int i = 0; i < children.size(); i++) {
            Element element = children.get(i);
            if (element.getLocalName().equalsIgnoreCase("product")) {
                Attribute idAttr = element.getAttribute("id");
                if (idAttr != null) {
                    ids.add(idAttr.getValue());
                } else {
                    log.warn("Could not find id attribute for product element: " + element.toString());
                }
            }
        }
        int end = Integer.valueOf(rootElement.getAttribute("end").getValue());
        int total = Integer.valueOf(rootElement.getAttribute("total").getValue());
        return new PageData(end, total, ids.build());
    }
    
    private static class PageData {
        private final int end;
        private final int total;
        private final ImmutableSet<String> ids;
        
        public PageData(int end, int total, ImmutableSet<String> ids) {
            this.end = end;
            this.total = total;
            this.ids = ids;
        }
    }
}
