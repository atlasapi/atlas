package org.atlasapi.remotesite.five;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.SimpleHttpClient;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FiveUpdaterTest {

    private FiveUpdater.Builder fiveUpdaterBuilder;

    public FiveUpdaterTest() {

        SimpleHttpClient mockedHttpClient = mock(SimpleHttpClient.class);

        ChannelResolver channelResolver = mock(ChannelResolver.class);

        when(channelResolver.fromUri(any(String.class)))
                .thenReturn(Maybe.fromPossibleNullValue(mock(Channel.class)));

        fiveUpdaterBuilder = FiveUpdater.builder()
                .withChannelResolver(channelResolver)
                .withContentResolver(mock(ContentResolver.class))
                .withContentWriter(mock(ContentWriter.class))
                .withLocationPolicyIds(mock(FiveLocationPolicyIds.class))
                .withSocketTimeout(0)
                .withHttpClient(mockedHttpClient);

    }

    @Test
    public void getApiCallForToday() {
        getAndTestApiCall(
                FiveUpdater.TimeFrame.TODAY,
                "1973-11-03T00:00:00Z",
                "1973-11-03T23:59:59Z"
        );
    }

    @Test
    public void getApiCallForPlusAndMinusSevenDays() {
        getAndTestApiCall(
                FiveUpdater.TimeFrame.PLUS_MINUS_7_DAYS,
                "1973-10-27T02:02:02Z",
                "1973-11-10T02:02:02Z"
        );
    }

    @Test
    public void getApiCallForAll() {
        FiveUpdater fiveUpdater = fiveUpdaterBuilder
                .withTimeFrame(FiveUpdater.TimeFrame.ALL)
                .build();

        String foundCall = fiveUpdater.getApiCall(0, "", DateTime.now());
        String expectedCall = "https://pdb.five.tv/internal/shows?offset=0&limit=100";

        assertEquals(expectedCall, foundCall);
    }

    private void getAndTestApiCall(
            FiveUpdater.TimeFrame timeFrame,
            String startDateTime,
            String endDateTime
    ) {
        FiveUpdater fiveUpdater = fiveUpdaterBuilder
                .withTimeFrame(timeFrame)
                .build();

        DateTimeFormatter formatter = DateTimeFormat.forPattern("y-M-d'T'hh:mm:ss'Z'");
        DateTime testDate = formatter
                .parseDateTime("1973-11-03T02:02:02Z")
                .withZone(DateTimeZone.UTC);

        String foundApiCall = fiveUpdater.getApiCall(0, "channelId", testDate);

        String predictedBaseUri = "https://pdb.five.tv/internal/channels/channelId/transmissions?"
                + "expand=watchable&from_time=%s&until_time=%s&limit=100&offset=0";

        String predictedUri = String.format(
                predictedBaseUri,
                startDateTime,
                endDateTime
        );

        assertEquals(predictedUri, foundApiCall);
    }
}
