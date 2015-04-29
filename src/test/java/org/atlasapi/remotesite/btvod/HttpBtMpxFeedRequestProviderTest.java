package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.SimpleHttpRequest;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class HttpBtMpxFeedRequestProviderTest {

    HttpBtMpxFeedRequestProvider objectUnderTest = new HttpBtMpxFeedRequestProvider("https://feed.product.theplatform.eu/f/CiIRPC/btv-prd-search");
    @Test
    public void testBuildRequest() throws Exception {
        SimpleHttpRequest<BtVodResponse> request = objectUnderTest.buildRequest(42);

        assertThat(request.getUrl(), is("https://feed.product.theplatform.eu/f/CiIRPC/btv-prd-search?startIndex=42"));
        assertThat(request.getTransformer(), instanceOf(BtVodResponseTransformer.class));
    }
}