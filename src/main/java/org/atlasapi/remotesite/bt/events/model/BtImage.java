package org.atlasapi.remotesite.bt.events.model;

import com.google.gson.annotations.SerializedName;


public class BtImage {

    @SerializedName("AssetType")
    private String assetType;
    @SerializedName("updateddate")
    private String updatedDate;
    @SerializedName("summaryimage")
    private String summaryImage;
    private String id;
    @SerializedName("Publist")
    private String publist;
    @SerializedName("filedesktop")
    private String fileDesktop;
    private String name;
    @SerializedName("heroimage")
    private String heroImage;
    @SerializedName("h1title")
    private String h1Title;
    @SerializedName("createddate")
    private String createdDate;
    @SerializedName("alttext")
    private String altText;
    private String subtype;
    @SerializedName("thumbnailimage")
    private String thumbnailImage;
    private String personal;
    @SerializedName("sourceType")
    private String sourceType;
    
    public BtImage() { }
    
    public String assetType() {
        return assetType;
    }

    public String updatedDate() {
        return updatedDate;
    }
    
    public String summaryImage() {
        return summaryImage;
    }
    
    public String id() {
        return id;
    }
    
    public String publist() {
        return publist;
    }
    
    public String fileDesktop() {
        return fileDesktop;
    }
    
    public String name() {
        return name;
    }

    public String heroImage() {
        return heroImage;
    }

    public String h1Title() {
        return h1Title;
    }

    public String createdDate() {
        return createdDate;
    }

    public String altText() {
        return altText;
    }

    public String subtype() {
        return subtype;
    }

    public String thumbnailImage() {
        return thumbnailImage;
    }

    public String personal() {
        return personal;
    }

    public String sourceType() {
        return sourceType;
    }
}
