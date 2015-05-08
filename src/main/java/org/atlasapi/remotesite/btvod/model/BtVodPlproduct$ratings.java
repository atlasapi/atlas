package org.atlasapi.remotesite.btvod.model;

import com.google.common.primitives.Ints;

public class BtVodPlproduct$ratings {

    private String plproduct$scheme;
    private String plproduct$rating;

    public BtVodPlproduct$ratings() {
    }

    public String getPlproduct$scheme() {
        return plproduct$scheme;
    }

    public void setPlproduct$scheme(String plproduct$scheme) {
        this.plproduct$scheme = plproduct$scheme;
    }

    public Integer getPlproduct$rating() {
        if (plproduct$rating == null) {
            return null;
        }
        try {
            return Ints.tryParse(plproduct$rating);
        } catch (Exception e) {
            return null;
        }
    }

    public String getPlproduct$ratingString() {
       return plproduct$rating;
    }

    public void setPlproduct$rating(String plproduct$rating) {
        this.plproduct$rating = plproduct$rating;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$ratings{" +
                "plproduct$scheme='" + plproduct$scheme + '\'' +
                ", plproduct$rating='" + plproduct$rating + '\'' +
                '}';
    }
}
