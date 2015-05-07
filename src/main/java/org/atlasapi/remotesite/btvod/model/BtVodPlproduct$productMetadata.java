package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

public class BtVodPlproduct$productMetadata {

    private String episodeNumber;
    private String audioDescription;
    private String releaseYear;

    @SerializedName("assetColorType.video-cardinal")
    private String assetColorTypeVideoCardinal;
    private String audioLanguage;
    private String clientAssetId;
    private String plproduct$title;
    private String plproduct$description;

    public BtVodPlproduct$productMetadata() {}

    public String getEpisodeNumber() {
        return episodeNumber;
    }

    public String getAudioDescription() {
        return audioDescription;
    }

    public String getReleaseYear() {
        return releaseYear;
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

    public String getPlproduct$title() {
        return plproduct$title;
    }

    public String getPlproduct$description() {
        return plproduct$description;
    }

    public void setEpisodeNumber(String episodeNumber) {
        this.episodeNumber = episodeNumber;
    }
}
