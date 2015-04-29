package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.HttpResponsePrologue;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;

import java.io.InputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class BtVodResponseTransformerTest {

    private  BtVodResponseTransformer objectUnderTest = new BtVodResponseTransformer();
    @Test
    public void testTransform() throws Exception {
        InputStream exampleResponse = this.getClass().getResourceAsStream("bt-vod-example.json");

        HttpResponsePrologue prologue = new HttpResponsePrologue(200);
        BtVodResponse btVodResponse = objectUnderTest.transform(prologue, exampleResponse);

        assertThat(btVodResponse.getEntries().size(), is(2));
        assertThat(btVodResponse.getStartIndex(), is(1));
        assertThat(btVodResponse.getEntryCount(), is(2));
        assertThat(btVodResponse.getItemsPerPage(), is(100));

        BtVodEntry entry1 = btVodResponse.getEntries().get(0);
        BtVodEntry entry2 = btVodResponse.getEntries().get(1);

        assertThat(
                entry1.getPlproduct$longDescription(),
                is("Go to Settings > channel scan<br><br>You will be offered a choice of region > select your correct region and channels should appear.")
        );

    }


}