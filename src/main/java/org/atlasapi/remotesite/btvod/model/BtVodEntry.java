package org.atlasapi.remotesite.btvod.model;

import java.util.List;

public class BtVodEntry {

    private static final String CONTENT_PROVIDER = "contentProvider";
    private String guid;
    private String title;
    private String description;
    private String plproduct$longDescription;
    private String btproduct$productType;
    private List<BtVodPlproduct$scopes> plproduct$scopes;
    private List<BtVodPlproduct$productTag> plproduct$productTags;
    private Long plproduct$offerStartDate;
    private Long plproduct$offerEndDate;
    private BtVodPlproduct$pricingPlan plproduct$pricingPlan;
    private Integer btproduct$priority;
    private String btproduct$targetBandwidth;

    public BtVodEntry() {}

    public String getGuid() {
        return guid;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getPlproduct$longDescription() {
        return plproduct$longDescription;
    }

    public String getBtproduct$productType() {
        return btproduct$productType;
    }

    public String getContentProviderId() {
        for (BtVodPlproduct$productTag plproduct$productTag : plproduct$productTags) {
            if (plproduct$productTag.getPlproduct$scheme().equals(CONTENT_PROVIDER)) {
                return plproduct$productTag.getPlproduct$title();
            }
        }

        return null;
    }

    public List<BtVodPlproduct$scopes> getPlproduct$scopes() {
        return plproduct$scopes;
    }

    public Long getPlproduct$offerStartDate() {
        return plproduct$offerStartDate;
    }

    public Long getPlproduct$offerEndDate() {
        return plproduct$offerEndDate;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setPlproduct$longDescription(String plproduct$longDescription) {
        this.plproduct$longDescription = plproduct$longDescription;
    }

    public void setBtproduct$productType(String btproduct$productType) {
        this.btproduct$productType = btproduct$productType;
    }

    public void setPlproduct$scopes(List<BtVodPlproduct$scopes> plproduct$scopes) {
        this.plproduct$scopes = plproduct$scopes;
    }

    public void setPlproduct$productTags(List<BtVodPlproduct$productTag> plproduct$productTags) {
        this.plproduct$productTags = plproduct$productTags;
    }

    public void setPlproduct$offerStartDate(Long plproduct$offerStartDate) {
        this.plproduct$offerStartDate = plproduct$offerStartDate;
    }

    public void setPlproduct$offerEndDate(Long plproduct$offerEndDate) {
        this.plproduct$offerEndDate = plproduct$offerEndDate;
    }

    public BtVodPlproduct$pricingPlan getPlproduct$pricingPlan() {
        return plproduct$pricingPlan;
    }

    public void setPlproduct$pricingPlan(BtVodPlproduct$pricingPlan plproduct$pricingPlan) {
        this.plproduct$pricingPlan = plproduct$pricingPlan;
    }

    public Integer getBtproduct$priority() {
        return btproduct$priority;
    }

    public void setBtproduct$priority(Integer btproduct$priority) {
        this.btproduct$priority = btproduct$priority;
    }

    public String getBtproduct$targetBandwidth() {
        return btproduct$targetBandwidth;
    }

    public void setBtproduct$targetBandwidth(String btproduct$targetBandwidth) {
        this.btproduct$targetBandwidth = btproduct$targetBandwidth;
    }
}
