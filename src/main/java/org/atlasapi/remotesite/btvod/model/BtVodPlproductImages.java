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

    @SerializedName("image-single-packshot-hd")
    private List<BtVodImage> packshotImagesHd;

    @SerializedName("image-double-packshot-hd")
    private List<BtVodImage> packshotDoubleImagesHd;
    
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
    
    public List<BtVodImage> getHdPackshotImages() {
        if (packshotImagesHd == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(packshotImagesHd);
    }
    
    public List<BtVodImage> getPackshotDoubleImages() {
        if (packshotDoubleImages == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(packshotDoubleImages);
    }
    
    public List<BtVodImage> getHdPackshotDoubleImages() {
        if (packshotDoubleImagesHd == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(packshotDoubleImagesHd);
    }

    public void setPackshotImages(List<BtVodImage> packshotImages) {
        this.packshotImages = packshotImages;
    }
    
    public void setPackshotDoubleImages(List<BtVodImage> packshotDoubleImages) {
        this.packshotDoubleImages = packshotDoubleImages;
    }
    
    public void setPackshotImagesHd(List<BtVodImage> packshotImagesHd) {
        this.packshotImagesHd = packshotImagesHd;
    }
    
    public void setPackshotDoubleImagesHd(List<BtVodImage> packshotDoubleImagesHd) {
        this.packshotDoubleImagesHd = packshotDoubleImagesHd;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$images{" +
                "backgroundImages=" + backgroundImages +
                ", packshotImages=" + packshotImages +
                '}';
    }
}
