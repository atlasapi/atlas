package org.atlasapi.remotesite.btvod.model;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BtVodPlproductImages {

    @SerializedName("image-background")
    private List<BtVodImage> backgroundImages;

    @SerializedName("image-single-packshot")
    private List<BtVodImage> packshotImages;

    @SerializedName("image-double-packshot")
    private List<BtVodImage> packshotDoubleImages;
    
    public BtVodPlproductImages() {
    }

    public List<BtVodImage> getBackgroundImages() {
        if (backgroundImages == null) {
            return ImmutableList.of();
        }
        return backgroundImages;
    }

    public void setBackgroundImages(List<BtVodImage> backgroundImages) {
        this.backgroundImages = backgroundImages;
    }

    public List<BtVodImage> getPackshotImages() {
        if (packshotImages == null) {
            return ImmutableList.of();
        }
        return packshotImages;
    }
    
    public List<BtVodImage> getPackshotDoubleImages() {
        if (packshotDoubleImages == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(packshotDoubleImages);
    }

    public void setPackshotImages(List<BtVodImage> packshotImages) {
        this.packshotImages = packshotImages;
    }
    
    public void setPackshotDoubleImages(List<BtVodImage> packshotDoubleImages) {
        this.packshotDoubleImages = packshotDoubleImages;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$images{" +
                "backgroundImages=" + backgroundImages +
                ", packshotImages=" + packshotImages +
                '}';
    }
}
