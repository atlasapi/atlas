package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

public class BtVodProductMetadata {

    private String episodeNumber;
    private String audioDescription;

    private String releaseYear;

    private String subGenres;
    @SerializedName("assetColorType.video-cardinal")
    private String assetColorTypeVideoCardinal;

    private String audioLanguage;
    private String clientAssetId;
    @SerializedName("plproduct$title")
    private String productTitle;

    @SerializedName("plproduct$description")
    private String productDescription;

    public BtVodProductMetadata() {}


    public String getEpisodeNumber() {
        return episodeNumber;
    }

    public String getAudioDescription() {
        return audioDescription;
    }

    public String getReleaseYear() {
        return releaseYear;
    }

    public String getSubGenres() {
        return subGenres;
    }

    public void setSubGenres(String subGenres) {
        this.subGenres = subGenres;
    }

    public String getAssetColorTypeVideoCardinal() {
        return assetColorTypeVideoCardinal;
    }

    public void setAssetColorTypeVideoCardinal(String assetColorTypeVideoCardinal) {
        this.assetColorTypeVideoCardinal = assetColorTypeVideoCardinal;
    }

    public String getAudioLanguage() {
        return audioLanguage;
    }

    public String getClientAssetId() {
        return clientAssetId;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public String getProductDescription() {
        return productDescription;
    }

    public void setEpisodeNumber(String episodeNumber) {
        this.episodeNumber = episodeNumber;
    }

    public void setReleaseYear(String releaseYear) {
        this.releaseYear = releaseYear;
    }
}
