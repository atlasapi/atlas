package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

public class BtVodProductPricingTier {

    @SerializedName("plproduct$absoluteStart")
    private Long productAbsoluteStart;

    @SerializedName("plproduct$absoluteEnd")
    private Long productAbsoluteEnd;

    @SerializedName("plproduct$amounts")
    private BtVodProductAmounts productAmounts;

    public BtVodProductPricingTier() {
    }

    public void setProductAbsoluteStart(Long productAbsoluteStart) {
        this.productAbsoluteStart = productAbsoluteStart;
    }

    public void setProductAbsoluteEnd(Long productAbsoluteEnd) {
        this.productAbsoluteEnd = productAbsoluteEnd;
    }

    public Long getProductAbsoluteStart() {
        return productAbsoluteStart;
    }

    public Long getProductAbsoluteEnd() {
        return productAbsoluteEnd;
    }

    public BtVodProductAmounts getProductAmounts() {
        return productAmounts;
    }

    public void setProductAmounts(BtVodProductAmounts productAmounts) {
        this.productAmounts = productAmounts;
    }
}
