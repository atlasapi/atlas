package org.atlasapi.remotesite.btvod.model;

import com.google.api.client.util.Lists;

import java.util.List;

public class BtVodPlproduct$pricingPlan {

    private List<BtVodPlproduct$pricingTier> plproduct$pricingTiers = Lists.newArrayList();

    public BtVodPlproduct$pricingPlan() {
    }

    public List<BtVodPlproduct$pricingTier> getPlproduct$pricingTiers() {
        return plproduct$pricingTiers;
    }

    public void setPlproduct$pricingTiers(List<BtVodPlproduct$pricingTier> plproduct$pricingTiers) {
        this.plproduct$pricingTiers = plproduct$pricingTiers;
    }
}
