package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

public class BtVodProductScope {

    @SerializedName("plproduct$scopeId")
    private String productScopeId;

    @SerializedName("plproduct$id")
    private String productId;

    @SerializedName("plproduct$title")
    private String productTitle;

    @SerializedName("plproduct$description")
    private String productDescription;

    @SerializedName("plproduct$fulfillmentStatus")
    private String productFulfillmentStatus;

    @SerializedName("plproduct$productMetadata")
    private BtVodProductMetadata productMetadata;

    public BtVodProductScope() {}

    public String getProductScopeId() {
        return productScopeId;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public String getProductDescription() {
        return productDescription;
    }

    public String getProductFulfillmentStatus() {
        return productFulfillmentStatus;
    }

    public BtVodProductMetadata getProductMetadata() {
        return productMetadata;
    }

    public void setProductMetadata(BtVodProductMetadata productMetadata) {
        this.productMetadata = productMetadata;
    }
}
