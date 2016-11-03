package org.atlasapi.remotesite.five;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.channel4.pmlsd.RequestLimitingRemoteSiteClient;

import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.base.Throwables;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FiveUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(FiveUpdater.class);
    private static final String BASE_API_URL = "https://pdb.five.tv/internal";
    private static final int LIMIT = 100;

    private final FiveBrandProcessor processor;
    private final Timestamper timestamper = new SystemClock();
    private final int socketTimeout;
    private final Builder parser = new Builder();
    private final SimpleHttpClient streamHttpClient;

    private int processedItems = 0;
    private int failedItems =  0;

    private FiveUpdater(
            ContentWriter contentWriter,
            ChannelResolver channelResolver,
            ContentResolver contentResolver,
            FiveLocationPolicyIds locationPolicyIds,
            int socketTimeout
    ) {
        this.socketTimeout = socketTimeout;
        this.streamHttpClient = buildFetcher();
        this.processor = FiveBrandProcessor.create(
                contentWriter,
                contentResolver,
                BASE_API_URL,
                new RequestLimitingRemoteSiteClient<>(
                        new HttpRemoteSiteClient(buildFetcher()),
                        20
                ),
                new FiveChannelMap(channelResolver),
                locationPolicyIds
        );
    }

    public static FiveUpdater create(
            ContentWriter contentWriter,
            ChannelResolver channelResolver,
            ContentResolver contentResolver,
            FiveLocationPolicyIds locationPolicyIds,
            int socketTimeout
    ) {
        return new FiveUpdater(
                contentWriter,
                channelResolver,
                contentResolver,
                locationPolicyIds,
                socketTimeout
        );
    }

    @Override
    public void runTask() {
        Timestamp start = timestamper.timestamp();
        try {
            log.info("Five update started from " + BASE_API_URL);

            boolean exhausted = false;
            int startingPoint = 0;

            while (!exhausted) {
                Document document = streamHttpClient.get(new SimpleHttpRequest<>(
                        getApiCall(startingPoint),
                        (responsePrologue, inputStream) -> parseResponse(inputStream)
                ));

                Elements shows = document.getRootElement()
                        .getFirstChildElement("shows")
                        .getChildElements();

                process(shows);

                startingPoint += LIMIT;
                exhausted = shows.size() == 0;
            }

            Timestamp end = timestamper.timestamp();
            log.info(
                    "Five update completed in {} seconds",
                    start.durationTo(end).getStandardSeconds()
            );
        }
        catch (NoHttpResponseException e) {
            Timestamp end = timestamper.timestamp();
            log.info(
                    "Five update failed in {} seconds",
                    start.durationTo(end).getStandardSeconds()
            );
            log.error("No response for target server. Could be due to timeout issue.", e);
            Throwables.propagate(e);
        }
        catch (Exception e) {
            Timestamp end = timestamper.timestamp();
            log.info(
                    "Five update failed in {} seconds",
                    start.durationTo(end).getStandardSeconds()
            );
            log.error("Exception when processing shows document",e);
            Throwables.propagate(e);
        }
    }

    public void updateBrand(String id) {
        try {
            Document document = streamHttpClient.get(new SimpleHttpRequest<>(
                    BASE_API_URL + "/shows/" + id,
                    (responsePrologue, inputStream) -> parseResponse(inputStream)
            ));
            process(document.getRootElement().getChildElements());
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    private SimpleHttpClient buildFetcher() {
        return new SimpleHttpClientBuilder()
                .withUserAgent(HttpClients.ATLAS_USER_AGENT)
                .withSocketTimeout(socketTimeout, TimeUnit.SECONDS)
                .withTrustUnverifiedCerts()
                .withRetries(3)
                .build();
    }

    private String getApiCall(int startingPoint) {
        return String.format("%s/shows?offset=%d&limit=%d", BASE_API_URL, startingPoint, LIMIT);
    }

    private void process(Elements elements) {

        for(int i = 0; i < elements.size(); i++) {
            Element element = elements.get(i);

            if (element.getLocalName().equalsIgnoreCase("show")) {
                try {
                    processor.processShow(element);
                }
                catch (Exception e) {
                    log.error("Exception when processing show", e);
                    failedItems++;
                }
                reportStatus(String.format(
                        "%s processed. %s failed",
                        ++processedItems,
                        failedItems
                ));
            }
        }
    }

    @Nullable
    private Document parseResponse(InputStream inputStream) {
        try {
            return parser.build(inputStream);
        } catch (Exception e) {
            log.error("Exception when processing shows document",e);
            Throwables.propagate(e);
        }
        return null;
    }
}
