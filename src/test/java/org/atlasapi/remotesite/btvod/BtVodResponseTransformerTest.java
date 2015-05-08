package org.atlasapi.remotesite.btvod;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.http.HttpResponsePrologue;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$pricingTier;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$ratings;
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
        assertThat(
                entry1.getPlproduct$pricingPlan().getPlproduct$pricingTiers().size(), is(1)
        );
        assertThat(entry1.getBtproduct$priority(), is(3));

        BtVodPlproduct$pricingTier entry1PricingTier = Iterables.getOnlyElement(entry1.getPlproduct$pricingPlan().getPlproduct$pricingTiers());

        assertThat(entry1PricingTier.getPlproduct$absoluteStart(), is(1401631320000L));
        assertThat(entry1PricingTier.getPlproduct$absoluteEnd(), is(1767139200000L));
        assertThat(entry1PricingTier.getPlproduct$amounts().getGBP(), is(0.5));

        BtVodPlproduct$ratings restriction = Iterables.getOnlyElement(entry1.getplproduct$ratings());

        assertThat(restriction.getPlproduct$rating(), is(12));
        assertThat(restriction.getPlproduct$scheme(), is("urn:www.bbfc.co.uk"));

        assertThat(entry2.getGenre(), is("Junior Girl and Boy"));

    }


}