package org.atlasapi.remotesite.bt.events.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;


/**
 * A simple wrapper for the BT JSON data format. This is designed to make 
 * deserialization easy, and make the conversion to Atlas' @{link org.atlasapi.media.entity.Event}
 * model as easy as possible.
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
public class BtEvent {

    private String location;
    @SerializedName("AssetType")
    private String assetType;
    @SerializedName("updateddate")
    private String updatedDate;
    private String id;
    @SerializedName("sportimage")
    private List<BtImage> sportImages;
    @SerializedName("Publist")
    private String publist;
    private String name;
    @SerializedName("h1title")
    private String h1Title;
    @SerializedName("createddate")
    private String createdDate;
    private String country;
    @SerializedName("startdate")
    private String startDate;
    private String teaser;
    private String subtype;
    @SerializedName("enddate")
    private String endDate;
    @SerializedName("sitearea")
    private String siteArea;
    private String url;
    @SerializedName("sourcetype")
    private String sourceType;
    @SerializedName("FeedItems")
    private String feedItems;
    
    public BtEvent() { }

    public String location() {
        return location;
    }

    public String assetType() {
        return assetType;
    }

    public String updatedDate() {
        return updatedDate;
    }

    public String id() {
        return id;
    }

    public List<BtImage> sportImages() {
        return sportImages;
    }

    public String publist() {
        return publist;
    }

    public String name() {
        return name;
    }

    public String h1Title() {
        return h1Title;
    }
    
    public String createdDate() {
        return createdDate;
    }

    public String country() {
        return country;
    }

    public String startDate() {
        return startDate;
    }

    public String teaser() {
        return teaser;
    }
    
    public String subtype() {
        return subtype;
    }

    public String endDate() {
        return endDate;
    }

    public String siteArea() {
        return siteArea;
    }

    public String url() {
        return url;
    }

    public String sourceType() {
        return sourceType;
    }

    public String feedItems() {
        return feedItems;
    }
    
}
