package org.atlasapi.remotesite.btvod.model;

import com.google.api.client.util.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BtVodProductPricingPlan {

    @SerializedName("plproduct$pricingTiers")
    private List<BtVodProductPricingTier> productPricingTiers = Lists.newArrayList();

    public BtVodProductPricingPlan() {
    }

    public List<BtVodProductPricingTier> getProductPricingTiers() {
        return productPricingTiers;
    }

    public void setProductPricingTiers(List<BtVodProductPricingTier> productPricingTiers) {
        this.productPricingTiers = productPricingTiers;
    }
}
