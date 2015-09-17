package org.atlasapi.remotesite.btvod.model;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BtVodPlproductImages {

    @SerializedName("image-background")
    private List<BtVodImage> backgroundImages;

    @SerializedName("image-single-packshot")
    private List<BtVodImage> singlePackshotImages;

    @SerializedName("image-double-packshot")
    private List<BtVodImage> doublePackshotImages;

    @SerializedName("image-single-packshot-hd")
    private List<BtVodImage> singlePackshotImagesHd;

    @SerializedName("image-double-packshot-hd")
    private List<BtVodImage> doublePackshotImagesHd;
    
    @SerializedName("image-single-packshot-hires")
    private List<BtVodImage> singlePackshotHires;
    
    @SerializedName("image-double-packshot-hires")
    private List<BtVodImage> doublePackshotHires;
    
    public BtVodPlproductImages() {
    }

    public List<BtVodImage> getSinglePackshotHiresImages() {
        if (singlePackshotHires == null) {
            return ImmutableList.of();
        }
        return singlePackshotHires;
    }
    
    public List<BtVodImage> getDoublePackshotHiresImages() {
        if (doublePackshotHires == null) {
            return ImmutableList.of();
        }
        return doublePackshotHires;
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

    public List<BtVodImage> getSinglePackshotImages() {
        if (singlePackshotImages == null) {
            return ImmutableList.of();
        }
        return singlePackshotImages;
    }
    
    public List<BtVodImage> getSinglePackshotImagesHd() {
        if (singlePackshotImagesHd == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(singlePackshotImagesHd);
    }
    
    public List<BtVodImage> getDoublePackshotImages() {
        if (doublePackshotImages == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(doublePackshotImages);
    }
    
    public List<BtVodImage> getDoublePackshotImagesHd() {
        if (doublePackshotImagesHd == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(doublePackshotImagesHd);
    }

    public void setSinglePackshotImages(List<BtVodImage> packshotImages) {
        this.singlePackshotImages = packshotImages;
    }
    
    public void setDoublePackshotImages(List<BtVodImage> packshotDoubleImages) {
        this.doublePackshotImages = packshotDoubleImages;
    }
    
    public void setSinglePackshotImagesHd(List<BtVodImage> packshotImagesHd) {
        this.singlePackshotImagesHd = packshotImagesHd;
    }
    
    public void setDoublePackshotImagesHd(List<BtVodImage> packshotDoubleImagesHd) {
        this.doublePackshotImagesHd = packshotDoubleImagesHd;
    }

    @Override
    public String toString() {
        return "BtVodPlproduct$images{" +
                "backgroundImages=" + backgroundImages +
                ", packshotImages=" + singlePackshotImages +
                '}';
    }
}
