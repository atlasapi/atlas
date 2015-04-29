package org.atlasapi.remotesite.btvod.model;

import com.google.gson.annotations.SerializedName;

public class BtVodPlproduct$productMetadata {

    private String episodeNumber;
    private String audioDescription;
    private String releaseYear;

    @SerializedName("assetColorType.video-cardinal")
    private String assetColorTypeVideoCardinal;
    private String audioLanguage;

    public BtVodPlproduct$productMetadata() {}
}
