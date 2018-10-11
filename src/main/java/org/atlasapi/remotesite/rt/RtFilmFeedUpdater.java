package org.atlasapi.remotesite.rt;

import static org.atlasapi.persistence.logging.AdapterLogEntry.errorEntry;

import java.io.InputStreamReader;

import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.NodeFactory;
import nu.xom.Nodes;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Throwables;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.url.UrlEncoding;

public class RtFilmFeedUpdater extends ScheduledTask {
    
    private final static DateTimeFormatter dateFormat = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
    private final static DateTime START_DATE = new DateTime(2013, DateTimeConstants.APRIL, 12, 0, 0, 0, 0);

    private final String feedUrl;
    private final AdapterLog log;
    private final RtFilmProcessor processor;
    private final boolean doCompleteUpdate;
    private final boolean doFourMonthUpdate;
    private final boolean doTwoYearUpdate;
    private OwlTelescopeReporter telescopeReporter;

    public RtFilmFeedUpdater(
            String feedUrl,
            AdapterLog log,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            RtFilmProcessor processor
    ) {
        this(feedUrl, log, contentResolver, contentWriter, processor, false, false, false);
    }
    
    private RtFilmFeedUpdater(
            String feedUrl,
            AdapterLog log,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            RtFilmProcessor processor,
            boolean doCompleteUpdate,
            boolean doFourMonthUpdate,
            boolean dotwoYearUpdate
    ) {
        this.feedUrl = feedUrl;
        this.log = log;
        this.processor = processor;
        this.doCompleteUpdate = doCompleteUpdate;
        this.doFourMonthUpdate = doFourMonthUpdate;
        this.doTwoYearUpdate = dotwoYearUpdate;
    }
    
    public static RtFilmFeedUpdater completeUpdater(
            String feedUrl,
            AdapterLog log,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            RtFilmProcessor processor
    ) {
        return new RtFilmFeedUpdater(feedUrl, log, contentResolver, contentWriter, processor, true, false, false);
    }

    public static RtFilmFeedUpdater fourMonthUpdater(
            String feedUrl,
            AdapterLog log,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            RtFilmProcessor processor
    ) {
        return new RtFilmFeedUpdater(feedUrl, log, contentResolver, contentWriter, processor, false, true, false);
    }

    public static RtFilmFeedUpdater twoYearUpdater(
            String feedUrl,
            AdapterLog log,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            RtFilmProcessor processor
    ){
        return new RtFilmFeedUpdater(feedUrl, log, contentResolver, contentWriter, processor, false, false, true);
    }

    @Override
    protected void runTask() {
        telescopeReporter = OwlTelescopeReporterFactory.getInstance()
                .getTelescopeReporter(
                        OwlTelescopeReporters.RADIO_TIMES_INGESTER,
                        Event.Type.INGEST);
        telescopeReporter.startReporting();

        String requestUri = feedUrl;

        if (doCompleteUpdate) {
            requestUri += "/since?lastUpdated=" + UrlEncoding.encode(dateFormat.print(START_DATE));
        } else if (doFourMonthUpdate) {
            requestUri += "/since?lastUpdated=" + UrlEncoding
                    .encode(dateFormat.print(new DateTime(DateTimeZone.UTC).minusMonths(4)));
        } else if (doTwoYearUpdate) {
            requestUri += "since?lastUpdate=" + UrlEncoding
                    .encode(dateFormat.print(new DateTime(DateTimeZone.UTC).minusYears(2)));
        } else {
            requestUri += "/since?lastUpdated=" + UrlEncoding
                    .encode(dateFormat.print(new DateTime(DateTimeZone.UTC).minusDays(3)));
        }
        
        try {
            reportStatus("Started...");
            getAndTransform(requestUri);
        } catch (Exception e) {
            AdapterLogEntry errorRecord = errorEntry()
                    .withCause(e)
                    .withSource(getClass())
                    .withUri(requestUri)
                    .withDescription("Exception while fetching film feed");
            log.record(errorRecord);
            reportStatus("Failed: " + errorRecord.id());
            telescopeReporter.reportFailedEvent("Failed with exception. Ingest stopped abruptly. " + e.getMessage(),
                    EntityType.FILM, requestUri, errorRecord);
            Throwables.propagate(e);
        } finally {
            telescopeReporter.endReporting();
        }
    }

    private void getAndTransform(String requestUri) throws Exception {
        CloseableHttpClient httpClient = HttpClients
                .custom()
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();

        HttpGet httpGet = new HttpGet(requestUri);

        CloseableHttpResponse response = httpClient.execute(httpGet);

        InputStreamReader responseStream = new InputStreamReader(response.getEntity().getContent());

        FilmProcessingNodeFactory filmProcessingNodeFactory = new FilmProcessingNodeFactory();
        Builder builder = new Builder(filmProcessingNodeFactory);
        builder.build(responseStream);
        reportStatus(String.format(
                "Finished. Processed %s. %s failed",
                filmProcessingNodeFactory.getProcessed(),
                filmProcessingNodeFactory.getFailed()
        ));
    }

    private class FilmProcessingNodeFactory extends NodeFactory {
        private int currentFilmNumber = 0;
        private int failures = 0;
        
        @Override
        public Nodes finishMakingElement(Element element) {
            if (element.getLocalName().equalsIgnoreCase("film") && shouldContinue()) {
                
                try {
                    processor.process(element, telescopeReporter);
                }
                catch (Exception e) {
                    log.record(new AdapterLogEntry(Severity.ERROR)
                            .withSource(RtFilmFeedUpdater.class)
                            .withCause(e)
                            .withDescription("Exception when processing film"));
                    failures++;
                    telescopeReporter.reportFailedEvent("Failed with exception. " + e.getMessage(),
                            EntityType.FILM, element);
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
