package org.atlasapi.remotesite.rt;

import static org.atlasapi.persistence.logging.AdapterLogEntry.errorEntry;

import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.NodeFactory;
import nu.xom.Nodes;

import org.atlasapi.http.AbstractHttpResponseTransformer;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.remotesite.HttpClients;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Throwables;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.url.UrlEncoding;

public class RtFilmFeedUpdater extends ScheduledTask {
    
    private final static DateTimeFormatter dateFormat = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
    private final static DateTime START_DATE = new DateTime(2011, DateTimeConstants.APRIL, 12, 0, 0, 0, 0);
    
    private final SimpleHttpClient client = new SimpleHttpClientBuilder()
        .withUserAgent(HttpClients.ATLAS_USER_AGENT)
        .withSocketTimeout(5, TimeUnit.MINUTES)
        .withConnectionTimeout(1, TimeUnit.MINUTES)
    .build();
    
    private final HttpResponseTransformer<Void> TRANSFORMER = new AbstractHttpResponseTransformer<Void>() {

        @Override
        protected Void transform(InputStreamReader bodyReader) throws Exception {
            try {
                FilmProcessingNodeFactory filmProcessingNodeFactory = new FilmProcessingNodeFactory();
                Builder builder = new Builder(filmProcessingNodeFactory);
                builder.build(bodyReader);
                reportStatus(String.format("Finished. Proessed %s. %s failed", filmProcessingNodeFactory.getProcessed(), filmProcessingNodeFactory.getFailed()));
            } catch (Exception e) {
                log.record(AdapterLogEntry.errorEntry().withCause(e).withSource(getClass()).withDescription("Exception in RT Film updater"));
            }
            return null;
        }
        
    };
    
    private final String feedUrl;
    private final AdapterLog log;
    private final RtFilmProcessor processor;
    private final boolean doCompleteUpdate;
    
    public RtFilmFeedUpdater(String feedUrl, AdapterLog log, ContentResolver contentResolver, ContentWriter contentWriter, RtFilmProcessor processor) {
        this(feedUrl, log, contentResolver, contentWriter, processor, false);
    }
    
    private RtFilmFeedUpdater(String feedUrl, AdapterLog log, ContentResolver contentResolver, ContentWriter contentWriter, RtFilmProcessor processor, boolean doCompleteUpdate) {
        this.feedUrl = feedUrl;
        this.log = log;
        this.processor = processor;
        this.doCompleteUpdate = doCompleteUpdate;
    }
    
    public static RtFilmFeedUpdater completeUpdater(String feedUrl, AdapterLog log, ContentResolver contentResolver, ContentWriter contentWriter, RtFilmProcessor processor) {
        return new RtFilmFeedUpdater(feedUrl, log, contentResolver, contentWriter, processor, true);
    }
    
    @Override
    protected void runTask() {
        String requestUri = feedUrl;
        
        if (doCompleteUpdate) {
            requestUri += "/since?lastUpdated=" + UrlEncoding.encode(dateFormat.print(START_DATE));
        } else {
            requestUri += "/since?lastUpdated=" + UrlEncoding.encode(dateFormat.print(new DateTime(DateTimeZone.UTC).minusDays(3)));
        }
        
        try {
            reportStatus("Started...");
            client.get(new SimpleHttpRequest<Void>(requestUri, TRANSFORMER));
        } catch (Exception e) {
            AdapterLogEntry errorRecord = errorEntry().withCause(e).withSource(getClass()).withUri(requestUri).withDescription("Exception while fetching film feed");
            log.record(errorRecord);
            reportStatus("Failed: " + errorRecord.id());
            Throwables.propagate(e);
        } 
    }

    private class FilmProcessingNodeFactory extends NodeFactory {
        private int currentFilmNumber = 0;
        private int failures = 0;
        
        @Override
        public Nodes finishMakingElement(Element element) {
            if (element.getLocalName().equalsIgnoreCase("film") && shouldContinue()) {
                
                try {
                    processor.process(element);
                }
                catch (Exception e) {
                    log.record(new AdapterLogEntry(Severity.ERROR).withSource(RtFilmFeedUpdater.class).withCause(e).withDescription("Exception when processing film"));
                    failures++;
                }
                
                reportStatus(String.format("Processing film number %s. %s failures ", ++currentFilmNumber, failures));
                return new Nodes();
            }
            else {
                return super.finishMakingElement(element);
            }
        }
        
        public int getProcessed() {
            return currentFilmNumber;
        }
        
        public int getFailed() {
            return failures;
        }
    }
}
