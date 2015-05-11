package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BtVodPlproduct$images {

    @SerializedName("image-background")
    private List<BtVodImage> backgroundImages;

    @SerializedName("image-single-packshot")
    private List<BtVodImage> packshotImages;

    public BtVodPlproduct$images() {
    }

    public List<BtVodImage> getBackgroundImages() {
        return backgroundImages;
    }

    public void setBackgroundImages(List<BtVodImage> backgroundImages) {
        this.backgroundImages = backgroundImages;
    }

    public List<BtVodImage> getPackshotImages() {
        return packshotImages;
    }

    public void setPackshotImages(List<BtVodImage> packshotImages) {
        this.packshotImages = packshotImages;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$images{" +
                "backgroundImages=" + backgroundImages +
                ", packshotImages=" + packshotImages +
                '}';
    }
}
