package org.atlasapi.remotesite.five;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;

import org.apache.commons.httpclient.NoHttpResponseException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.channel4.RequestLimitingRemoteSiteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponse;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

public class FiveUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(FiveUpdater.class);
    private static final String BASE_API_URL = "https://pdb.five.tv/internal";
    public static final int LIMIT = 100;
    private final FiveBrandProcessor processor;
    private final Timestamper timestamper = new SystemClock();
    private final int socketTimeout;

    private final Builder parser = new Builder();
    private SimpleHttpClient streamHttpClient;

    public FiveUpdater(ContentWriter contentWriter, ChannelResolver channelResolver, ContentResolver contentResolver, 
            FiveLocationPolicyIds locationPolicyIds, int socketTimeout) {
        this.socketTimeout = socketTimeout;
        this.streamHttpClient = buildFetcher();
        this.processor = new FiveBrandProcessor(contentWriter, contentResolver, BASE_API_URL, 
            new RequestLimitingRemoteSiteClient<HttpResponse>(new HttpRemoteSiteClient(buildFetcher()), 20), 
            channelMap(channelResolver), locationPolicyIds
        );
    }

    private Multimap<String, Channel> channelMap(ChannelResolver channelResolver) {
        return new FiveChannelMap(channelResolver); 
    }

    private SimpleHttpClient buildFetcher() {
        return new SimpleHttpClientBuilder()
            .withUserAgent(HttpClients.ATLAS_USER_AGENT)
            .withSocketTimeout(socketTimeout, TimeUnit.SECONDS)
            .withTrustUnverifiedCerts()
            .withRetries(3)
            .build();
    }
    
    private final HttpResponseTransformer<Document> TRANSFORMER = new HttpResponseTransformer<Document>() {
        @Override
        public Document transform(HttpResponsePrologue response, InputStream in) throws HttpException, IOException {
            try {
                return parser.build(in);
            } catch (Exception e) {
                log.error("Exception when processing shows document",e);
                Throwables.propagate(e);
            }
            return null;
        }
    };

    public void updateBrand(String id) {
        try {
            Document document = streamHttpClient.get(new SimpleHttpRequest<Document>(BASE_API_URL + "/shows/" + id, TRANSFORMER));
            process(document.getRootElement().getChildElements());
        } catch (HttpException e) {
            Throwables.propagate(e);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }
    
    @Override
    public void runTask() {
        Timestamp start = timestamper.timestamp();
        try {
            log.info("Five update started from " + BASE_API_URL);
            boolean exhausted = false;
            int startingPoint = 0;
            while (!exhausted) {
                String apiCall = getApiCall(startingPoint);
                Document document = streamHttpClient.get(new SimpleHttpRequest<Document>(apiCall, TRANSFORMER));
                Elements shows = document.getRootElement()
                        .getFirstChildElement("shows")
                        .getChildElements();
                process(shows);
                startingPoint += LIMIT;
                exhausted = shows.size() == 0;
            }


            Timestamp end = timestamper.timestamp();
            log.info("Five update completed in " + start.durationTo(end).getStandardSeconds() + " seconds");
        }
        catch (NoHttpResponseException e) {
            Timestamp end = timestamper.timestamp();
            log.info("Five update failed in " + start.durationTo(end).getStandardSeconds() + " seconds");
            log.error("No response for target server. Could be due to timeout issue.", e);
            Throwables.propagate(e);
        }
        catch (Exception e) {
            Timestamp end = timestamper.timestamp();
            log.info("Five update failed in " + start.durationTo(end).getStandardSeconds() + " seconds");
            log.error("Exception when processing shows document",e);
            Throwables.propagate(e);
        }
    }

    private String getApiCall(int startingPoint) {
        return String.format("%s/shows?offset=%d&limit=%d", BASE_API_URL, startingPoint, LIMIT);
    }

    private void process(Elements elements) {
        int processed = 0, failed = 0;
        
        for(int i = 0; i < elements.size(); i++) {
            Element element = elements.get(i);
        
            if (element.getLocalName().equalsIgnoreCase("show")) {
                try {
                    processor.processShow(element);
                }
                catch (Exception e) {
                    log.error("Exception when processing show", e);
                    failed++;
                }
                reportStatus(String.format("%s processed. %s failed", ++processed, failed));
            }
        }
    }

}
