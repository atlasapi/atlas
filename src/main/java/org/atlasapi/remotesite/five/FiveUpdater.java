package org.atlasapi.remotesite.five;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;

import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FiveUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(FiveUpdater.class);
    private static final String BASE_API_URL = "https://pdb.five.tv/internal";
    private static final int LIMIT = 100;

    private final FiveBrandProcessor showProcessor;
    private final FiveBrandWatchablesProcessor watchablesProcessor;
    private final Timestamper timestamper = new SystemClock();
    private final nu.xom.Builder parser = new nu.xom.Builder();
    private final CloseableHttpClient httpClient;

    private int processedItems = 0;
    private int failedItems =  0;
    private TimeFrame timeFrame;

    private FiveUpdater(Builder builder) {
        this.httpClient = builder.httpClient;
        this.showProcessor = FiveBrandProcessor.create(
                builder.contentWriter,
                builder.contentResolver,
                BASE_API_URL,
                builder.httpClient,
                new FiveChannelMap(builder.channelResolver),
                builder.locationPolicyIds
        );
        this.timeFrame = builder.timeFrame;
        this.watchablesProcessor = FiveBrandWatchablesProcessor.builder()
                .withWriter(builder.contentWriter)
                .withContentResolver(builder.contentResolver)
                .withBaseApiUrl(BASE_API_URL)
                .withHttpClient(builder.httpClient)
                .withLocationPolicyIds(builder.locationPolicyIds)
                .withChannelMap(new FiveChannelMap(builder.channelResolver))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void runTask() {
        Timestamp start = timestamper.timestamp();
        try {
            log.info("Five update started from " + BASE_API_URL);

            boolean exhausted = false;
            int startingPoint = 0;

            while (!exhausted) {
                List<Element> shows = fetchAndCombineElements(startingPoint);

                if (timeFrame == TimeFrame.ALL) {
                    processShows(shows, showProcessor, "show");
                } else {
                    processShows(shows, watchablesProcessor, "transmission");
                }

                startingPoint += LIMIT;
                exhausted = shows.size() == 0;
            }

            httpClient.close();

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

    private List<Element> fetchAndCombineElements(int startingPoint) throws Exception {
        if (timeFrame == TimeFrame.ALL) {
            return getElementsForAll(startingPoint);
        }

        List<Element> elementArrayList = Lists.newArrayList();
        Document channelIdList = getChannelIdList();
        // TODO: check channelIdList isn't null, return optional empty if it is (RETRY?? SHOULD IT ALWAYS NOT BE NULL?)

        Elements channelIds = channelIdList
                .getRootElement()
                .getFirstChildElement("channels")
                .getChildElements("channel");

        for (int i = 0; i < channelIds.size(); i++) {
            Element id = channelIds.get(i).getFirstChildElement("id");
            Optional<Elements> elementsOptional = getShowElements(id.getValue(), startingPoint);
            if (elementsOptional.isPresent()) {
                for (int j = 0; j < elementsOptional.get().size(); j++) {
                    Element element = elementsOptional.get().get(j);
                    elementArrayList.add(element);
                }
            }
        }

        return ImmutableList.copyOf(elementArrayList);
    }

    private Optional<Elements> getShowElements(String id, int startingPoint) throws Exception{

        if (id.contains(" ")) {
            return Optional.empty();
        }

        HttpGet request = new HttpGet(getApiCall(startingPoint, id, DateTime.now()));
        HttpResponse response = httpClient.execute(request);
        Document document = parser.build(new InputStreamReader(response.getEntity().getContent()));

        if (document.getRootElement()
                .getFirstChildElement("transmissions")
                .getChildElements("transmission").size() == 0) {
            return Optional.empty();
        }

        return Optional.of(document.getRootElement()
                .getFirstChildElement("transmissions")
                .getChildElements("transmission"));
    }

    private List<Element> getElementsForAll(int startingPoint) throws Exception {
        List<Element> elementArrayList = Lists.newArrayList();
        Optional<Elements> elementsOptional = getElements(startingPoint);

        if (!elementsOptional.isPresent()) {
            return ImmutableList.copyOf(elementArrayList);
        }

        for (int i = 0; i < elementsOptional.get().size(); i++) {
            elementArrayList.add(elementsOptional.get().get(i));
        }
        return ImmutableList.copyOf(elementArrayList);
    }

    private Document getChannelIdList() throws Exception {

        HttpGet request = new HttpGet("https://pdb.five.tv/internal/channels");
        HttpResponse response = httpClient.execute(request);
        return parser.build(new InputStreamReader(response.getEntity().getContent()));
    }

    private Optional<Elements> getElements(int startingPoint) throws Exception{
        HttpGet request = new HttpGet(getApiCall(startingPoint, "", DateTime.now()));
        HttpResponse response = httpClient.execute(request);
        Document document = parser.build(new InputStreamReader(response.getEntity().getContent()));

        if ((document.getRootElement().getFirstChildElement("shows") == null)) {
            return Optional.empty();
        }

        return Optional.of(document.getRootElement()
                .getFirstChildElement("shows")
                .getChildElements());
    }

    public void updateBrand(String id) {
        try {
            HttpGet request = new HttpGet(BASE_API_URL + "/shows/" + id);
            HttpResponse response = httpClient.execute(request);
            Document document = parser.build(new InputStreamReader(response.getEntity().getContent()));

            ArrayList<Element> elementArrayList = Lists.newArrayList();
            Elements elements = document.getRootElement().getChildElements();
            for (int i = 0; i < elements.size(); i++) {
                elementArrayList.add(elements.get(i));
            }
            if (timeFrame == TimeFrame.ALL) {
                processShows(elementArrayList, showProcessor, "show");
            } else {
                processShows(elementArrayList, watchablesProcessor, "transmission");
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    String getApiCall(int startingPoint, String channelId, DateTime dateTime) {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis();
        String baseUri = "https://pdb.five.tv/internal/channels/%s/transmissions"
                + "?expand=watchable&from_time=%s&until_time=%s";

        String encodedId = "";
        try {
            encodedId = URLEncoder.encode(channelId, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("encoding url failed", e);
        }

        if (timeFrame == TimeFrame.TODAY) {
            DateTime todayStartDateTime = dateTime
                    .withTimeAtStartOfDay()
                    .withZone(DateTimeZone.UTC);
            DateTime todayEndDateTime = todayStartDateTime
                    .plusDays(1)
                    .withTimeAtStartOfDay()
                    .minusSeconds(1);
            String todayStart = todayStartDateTime.toString(formatter);
            String todayFinish = todayEndDateTime.toString(formatter);

            return String.format(
                    baseUri,
                    encodedId,
                    todayStart,
                    todayFinish + "&limit=100&offset=" + Integer.toString(startingPoint)
            );

        } else if (timeFrame == TimeFrame.PLUS_MINUS_7_DAYS) {
            DateTime weekAgo = dateTime.minusDays(7);
            DateTime weekAhead = weekAgo.plusDays(14);
            String sevenDaysAgo = weekAgo.toString(formatter);
            String sevenDaysAhead = weekAhead.toString(formatter);

            return String.format(
                    baseUri,
                    encodedId,
                    sevenDaysAgo,
                    sevenDaysAhead + "&limit=100&offset=" + Integer.toString(startingPoint)
            );

        } else if (timeFrame == TimeFrame.NEW_ALL) {
            //TODO split this method up, it's huge
            return String.format("https://pdb.five.tv/internal/channels/C6P1/transmissions?"
                            + "expand=watchable&limit=%s&offset=%s",
                    "100",
                    Integer.toString(startingPoint)
            );
        } else {
            return String.format(
                    "%s/shows?offset=%d&limit=%d",
                    BASE_API_URL,
                    startingPoint,
                    LIMIT
            );
        }
    }

    private void processShows(
            List<Element> elements,
            FiveBrandProcessor processor,
            String childName
    ) {
        for(int i = 0; i < elements.size(); i++) {
            Element element = elements.get(i);

            int showsProcessed = 0;

            if (element.getLocalName().equalsIgnoreCase(childName)) {
                try {
                    showsProcessed += processor.processShow(element);
                }
                catch (Exception e) {
                    log.error("Exception when processing show", e);
                    failedItems++;
                }
                if (timeFrame == TimeFrame.ALL) {
                    reportStatus(String.format(
                            "%s processed. %s failed",
                            ++processedItems,
                            failedItems
                    ));
                } else {
                    processedItems += showsProcessed;
                    reportStatus(String.format(
                            "%s processed. %s failed",
                            processedItems,
                            failedItems
                    ));
                }
            }
        }
    }

    public enum TimeFrame {
        TODAY,
        PLUS_MINUS_7_DAYS,
        ALL,
        NEW_ALL

    }

    public static final class Builder {

        private ContentWriter contentWriter;
        private ChannelResolver channelResolver;
        private ContentResolver contentResolver;
        private FiveLocationPolicyIds locationPolicyIds;
        private TimeFrame timeFrame;
        private CloseableHttpClient httpClient;

        private Builder() {
        }

        public Builder withContentWriter(ContentWriter contentWriter) {
            this.contentWriter = contentWriter;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withContentResolver(ContentResolver contentResolver) {
            this.contentResolver = contentResolver;
            return this;
        }

        public Builder withLocationPolicyIds(FiveLocationPolicyIds locationPolicyIds) {
            this.locationPolicyIds = locationPolicyIds;
            return this;
        }

        public Builder withTimeFrame(TimeFrame timeFrame) {
            this.timeFrame = timeFrame;
            return this;
        }

        public Builder withHttpClient(CloseableHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        private CloseableHttpClient buildDefaultFetcher() {
            HttpRequestRetryHandler retryHandler = new StandardHttpRequestRetryHandler();
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(20000)
                    .setSocketTimeout(20000)
                    .build();
            return HttpClients
                    .custom()
                    .setRetryHandler(retryHandler)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
        }

        public FiveUpdater build() {
            if (httpClient == null) {
                httpClient = buildDefaultFetcher();
            }
            return new FiveUpdater(this);
        }
    }
}
