package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.SimpleHttpRequest;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class HttpBtMpxFeedRequestProviderTest {


    @Test
    public void testBuildRequest() throws Exception {
        HttpBtMpxFeedRequestProvider objectUnderTest = new HttpBtMpxFeedRequestProvider(
                "https://example.org/base/", "btv-prd-search",
                "-(productTagFullTitles:%22schedulerChannel:Music%22%20OR%20productTagFullTitles:%22contentProvider:SKY%22%20OR%20productType:%22help%22)"
        );

        SimpleHttpRequest<BtVodResponse> request = objectUnderTest.buildRequestForFeed("btv-prd-search", 42, 142);

        assertThat(request.getUrl(), is("https://example.org/base/btv-prd-search?range=42-142&q=-(productTagFullTitles:%22schedulerChannel:Music%22%20OR%20productTagFullTitles:%22contentProvider:SKY%22%20OR%20productType:%22help%22)"));
        assertThat(request.getTransformer(), instanceOf(BtVodResponseTransformer.class));
    }


    @Test
    public void testBuildRequestWithoutQueryParam() throws Exception {
        HttpBtMpxFeedRequestProvider objectUnderTest = new HttpBtMpxFeedRequestProvider(
                "https://example.org/base/", "btv-prd-search",
                ""
        );

        SimpleHttpRequest<BtVodResponse> request = objectUnderTest.buildRequestForFeed("btv-prd-search", 42, 142);

        assertThat(request.getUrl(), is("https://example.org/base/btv-prd-search?range=42-142"));
        assertThat(request.getTransformer(), instanceOf(BtVodResponseTransformer.class));
    }
}