package org.atlasapi.remotesite.btvod;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.http.HttpResponsePrologue;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
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
                entry1.getProductLongDescription(),
                is("Go to Settings > channel scan<br><br>You will be offered a choice of region > select your correct region and channels should appear.")
        );
        assertThat(
                entry1.getProductPricingPlan().getProductPricingTiers().size(), is(1)
        );
        assertThat(entry1.getProductPriority(), is(3));

        BtVodProductPricingTier entry1PricingTier = Iterables.getOnlyElement(entry1.getProductPricingPlan().getProductPricingTiers());

        assertThat(entry1PricingTier.getProductAbsoluteStart(), is(1401631320000L));
        assertThat(entry1PricingTier.getProductAbsoluteEnd(), is(1767139200000L));
        assertThat(entry1PricingTier.getProductAmounts().getGBP(), is(0.5));


        BtVodProductRating restriction = Iterables.getOnlyElement(entry1.getplproduct$ratings());

        assertThat(restriction.getProductRating(), is(12));
        assertThat(restriction.getProductScheme(), is("urn:www.bbfc.co.uk"));

        BtVodPlproductImages images = entry1.getProductImages();

        BtVodImage packshotImage = Iterables.getOnlyElement(images.getPackshotImages());
        BtVodImage backgroundImage = Iterables.getOnlyElement(images.getBackgroundImages());

        assertThat(packshotImage.getPlproduct$mediaFileId(), is("http://bt.data.media.theplatform.eu/media/data/MediaFile/4828741146"));
        assertThat(packshotImage.getPlproduct$height(), is(120));
        assertThat(packshotImage.getPlproduct$width(), is(214));
        assertThat(packshotImage.getPlproduct$url(), is("content_providers/images/BBJ374303A_374303_SP.png"));

        assertThat(backgroundImage.getPlproduct$mediaFileId(), is("http://bt.data.media.theplatform.eu/media/data/MediaFile/4829253140"));
        assertThat(backgroundImage.getPlproduct$height(), is(626));
        assertThat(backgroundImage.getPlproduct$width(), is(1159));
        assertThat(backgroundImage.getPlproduct$url(), is("content_providers/images/BBJ374303A_374303_WP.png"));

        assertThat(entry2.getGenre(), is("Junior Girl and Boy"));

    }


}