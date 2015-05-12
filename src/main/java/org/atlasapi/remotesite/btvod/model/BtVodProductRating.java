package org.atlasapi.remotesite.btvod.model;

import com.google.common.primitives.Ints;
import com.google.gson.annotations.SerializedName;

public class BtVodProductRating {

    @SerializedName("plproduct$scheme")
    private String productScheme;

    @SerializedName("plproduct$rating")
    private String productRating;

    public BtVodProductRating() {
    }

    public String getProductScheme() {
        return productScheme;
    }

    public void setProductScheme(String productScheme) {
        this.productScheme = productScheme;
    }

    public Integer getProductRating() {
        if (productRating == null) {
            return null;
        }
        try {
            return Ints.tryParse(productRating);
        } catch (Exception e) {
            return null;
        }
    }

    public String getPlproduct$ratingString() {
       return productRating;
    }

    public void setProductRating(String productRating) {
        this.productRating = productRating;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$ratings{" +
                "plproduct$scheme='" + productScheme + '\'' +
                ", plproduct$rating='" + productRating + '\'' +
                '}';
    }
}
