package org.atlasapi.remotesite.btvod.model;

import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

public class BtVodEntry {

    private static final String CONTENT_PROVIDER = "contentProvider";
    private static final String GENRE = "genre";
    private static final String SUBSCRIPTION_PRODUCT_SCHEME = "subscription";
    private static final String SCHEDULER_CHANNEL = "schedulerChannel";
    private static final String TRAILER_SERVICE_TYPE_SCHEME = "trailerServiceType";
    private static final String SERVICE_TYPE_SCHEME = "serviceType";
    private static final String MASTER_AGREEMENT_SERVICE_TYPE = "masterAgreementServiceType";
    private static final String MASTER_AGREEMENT_OTG_TVOD_PLAY_SCHEME = "masterAgreementOtgTvodPlay";
    private static final String KEYWORD = "keyword";
    
    private String id;
    private String guid;
    private String title;
    private Long added;
    private String description;

    @SerializedName("plproduct$longDescription")
    private String productLongDescription;

    @SerializedName("btproduct$productType")
    private String productType;

    @SerializedName("plproduct$scopes")
    private List<BtVodProductScope> productScopes;

    @SerializedName("plproduct$productTags")
    private List<BtVodPlproduct$productTag> productTags;

    @SerializedName("plproduct$offerStartDate")
    private Long productOfferStartDate;

    @SerializedName("plproduct$offerEndDate")
    private Long productOfferEndDate;

    @SerializedName("plproduct$pricingPlan")
    private BtVodProductPricingPlan productPricingPlan;

    @SerializedName("btproduct$priority")
    private Integer productPriority;

    @SerializedName("plproduct$ratings")
    private List<BtVodProductRating> productRatings;

    @SerializedName("plproduct$images")
    private BtVodPlproductImages productImages;

    @SerializedName("btproduct$targetBandwidth")
    private String productTargetBandwidth;

    @SerializedName("btproduct$trailerMediaId")
    private String productTrailerMediaId;

    @SerializedName("btproduct$duration")
    private Long productDuration;
    
    @SerializedName("btproduct$productOfferingType")
    private String productOfferingType;

    @SerializedName("btproduct$seriesNumber")
    private Integer seriesNumber;

    @SerializedName("btproduct$parentGuid")
    private String parentGuid;

    public BtVodEntry() {}

    public String getGuid() {
        return guid;
    }

    public String getTitle() {
        return title;
    }

    public Long getAdded() {
        return added;
    }

    public String getDescription() {
        return description;
    }

    public String getProductLongDescription() {
        return productLongDescription;
    }

    public String getProductType() {
        return productType;
    }

    public String getProductOfferingType() {
        return productOfferingType;
    }

    public void setProductOfferingType(String offeringType) {
        this.productOfferingType = offeringType;
    }

    public String getContentProviderId() {
        return productTag(CONTENT_PROVIDER);
    }

    /**
     * Platforms the trailer media is available on
     */
    public ImmutableSet<String> getTrailerServiceTypes() {
        return productTags(TRAILER_SERVICE_TYPE_SCHEME);
    }

    /**
     * Platforms the content media is available on
     */
    public ImmutableSet<String> getServiceTypes() {
        return productTags(SERVICE_TYPE_SCHEME);
    }

    private String productTag(String tag) {
        for (BtVodPlproduct$productTag plproduct$productTag : productTags) {
            if (plproduct$productTag.getPlproduct$scheme().equals(tag)) {
                return plproduct$productTag.getPlproduct$title();
            }
        }

        return null;
    }

    private ImmutableSet<String> productTags(String tag) {
        ImmutableSet.Builder<String> tags = ImmutableSet.builder();
        for (BtVodPlproduct$productTag plproduct$productTag : productTags) {
            if (plproduct$productTag.getPlproduct$scheme().equals(tag)) {
                tags.add(plproduct$productTag.getPlproduct$title());
            }
        }
        return tags.build();
    }

    public String getGenre() {
        return productTag(GENRE);
    }

    public ImmutableSet<String> getSubscriptionCodes() {
        return productTags(SUBSCRIPTION_PRODUCT_SCHEME);
    }

    public ImmutableSet<String> getMasterAgreementServiceTypes() {
        return productTags(MASTER_AGREEMENT_SERVICE_TYPE);
    }

    public String getMasterAgreementOtgTvodPlay() {
        return productTag(MASTER_AGREEMENT_OTG_TVOD_PLAY_SCHEME);
    }

    public ImmutableSet<String> getKeywordTags() {
        return productTags(KEYWORD);
    }

    public String getSchedulerChannel() {
        for (BtVodPlproduct$productTag plproduct$productTag : productTags) {
            if (plproduct$productTag.getPlproduct$scheme().equals(SCHEDULER_CHANNEL)) {
                return plproduct$productTag.getPlproduct$title();
            }
        }
        return null;
    }

    public List<BtVodProductScope> getProductScopes() {
        return productScopes;
    }

    public Long getProductOfferStartDate() {
        return productOfferStartDate;
    }

    public Long getProductOfferEndDate() {
        return productOfferEndDate;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setAdded(Long added) {
        this.added = added;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setProductDuration(Long productDuration) {
        this.productDuration = productDuration;
    }

    public Long getProductDuration() {
        return productDuration;
    }

    public void setProductLongDescription(String productLongDescription) {
        this.productLongDescription = productLongDescription;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public void setProductScopes(List<BtVodProductScope> productScopes) {
        this.productScopes = productScopes;
    }

    public void setProductTags(List<BtVodPlproduct$productTag> productTags) {
        this.productTags = productTags;
    }

    public void setProductOfferStartDate(Long productOfferStartDate) {
        this.productOfferStartDate = productOfferStartDate;
    }

    public void setProductOfferEndDate(Long productOfferEndDate) {
        this.productOfferEndDate = productOfferEndDate;
    }

    public BtVodProductPricingPlan getProductPricingPlan() {
        return productPricingPlan;
    }

    public void setProductPricingPlan(BtVodProductPricingPlan productPricingPlan) {
        this.productPricingPlan = productPricingPlan;
    }

    public Integer getProductPriority() {
        return productPriority;
    }

    public void setProductPriority(Integer productPriority) {
        this.productPriority = productPriority;
    }

    public String getProductTargetBandwidth() {
        return productTargetBandwidth;
    }

    public void setProductTargetBandwidth(String productTargetBandwidth) {
        this.productTargetBandwidth = productTargetBandwidth;
    }

    public List<BtVodProductRating> getplproduct$ratings() {
        return productRatings;
    }

    public void setProductRatings(List<BtVodProductRating> productRatings) {
        this.productRatings = productRatings;
    }

    public BtVodPlproductImages getProductImages() {
        return productImages;
    }

    public void setProductImages(BtVodPlproductImages productImages) {
        this.productImages = productImages;
    }

    @Override
    public String toString() {
        return "BtVodEntry{" +
                "guid='" + guid + '\'' +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                ", plproduct$longDescription='" + productLongDescription + '\'' +
                ", btproduct$productType='" + productType + '\'' +
                ", plproduct$scopes=" + productScopes +
                ", plproduct$productTags=" + productTags +
                ", plproduct$offerStartDate=" + productOfferStartDate +
                ", plproduct$offerEndDate=" + productOfferEndDate +
                ", plproduct$pricingPlan=" + productPricingPlan +
                ", btproduct$priority=" + productPriority +
                ", plproduct$ratings=" + productRatings +
                ", plproduct$images=" + productImages +
                ", btproduct$targetBandwidth='" + productTargetBandwidth + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProductTrailerMediaId() {
        return productTrailerMediaId;
    }

    public void setProductTrailerMediaId(String productTrailerMediaId) {
        this.productTrailerMediaId = productTrailerMediaId;
    }

    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public String getParentGuid() {
        return parentGuid;
    }

    public void setParentGuid(String parentGuid) {
        this.parentGuid = parentGuid;
    }
}
