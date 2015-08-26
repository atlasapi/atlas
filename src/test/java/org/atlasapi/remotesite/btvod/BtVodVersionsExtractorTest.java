package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.HttpResponsePrologue;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;

import java.io.InputStream;
import java.util.Set;

import static org.junit.Assert.*;

public class BtVodVersionsExtractorTest {

    private  BtVodResponseTransformer responseTransformer = new BtVodResponseTransformer();
    private BtVodVersionsExtractor btVodVersionsExtractor = new BtVodVersionsExtractor(new BtVodPricingAvailabilityGrouper(), "URI_PREFIX");

    @Test
    public void testCreateVersions() throws Exception {
        InputStream exampleResponse = this.getClass().getResourceAsStream("bt-vod-example-series-with-versions.json");

        HttpResponsePrologue prologue = new HttpResponsePrologue(200);
        BtVodResponse btVodResponse = responseTransformer.transform(prologue, exampleResponse);
        BtVodEntry series = btVodResponse.getEntries().get(0);
        Set<Version> versions = btVodVersionsExtractor.createVersions(series);

        return;
    }

}