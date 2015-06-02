package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.SimpleHttpRequest;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class HttpBtMpxFeedRequestProviderTest {

    HttpBtMpxFeedRequestProvider objectUnderTest = new HttpBtMpxFeedRequestProvider("https://example.org/base/");
    @Test
    public void testBuildRequest() throws Exception {
        SimpleHttpRequest<BtVodResponse> request = objectUnderTest.buildRequest("btv-prd-search", 42);

        assertThat(request.getUrl(), is("https://example.org/base/btv-prd-search?startIndex=42"));
        assertThat(request.getTransformer(), instanceOf(BtVodResponseTransformer.class));
    }
}