package org.atlasapi.remotesite.bbc.nitro.channels.hax;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class YouviewMasterbrand {

    private final String name;
    private final String shortName;
    private final String imageIdent;
    private final Integer widthIdent;
    private final Integer heightIdent;
    private final String imageDog;
    private final Integer widthDog;
    private final Integer heightDog;

    @JsonCreator
    static YouviewMasterbrand create(
            @JsonProperty("name") String name,
            @JsonProperty("shortName") String shortName,
            @JsonProperty("imageIdent") String imageIdent,
            @JsonProperty("widthIdent") Integer widthIdent,
            @JsonProperty("heightIdent") Integer heightIdent,
            @JsonProperty("imageDog") String imageDog,
            @JsonProperty("widthDog") Integer widthDog,
            @JsonProperty("heightDog") Integer heightDog
    ) {
        return new YouviewMasterbrand(
                name,
                shortName,
                imageIdent,
                widthIdent,
                heightIdent,
                imageDog,
                widthDog,
                heightDog
        );
    }

    private YouviewMasterbrand(
            String name,
            String shortName,
            String imageIdent,
            Integer widthIdent,
            Integer heightIdent,
            String imageDog,
            Integer widthDog,
            Integer heightDog
    ) {
        this.name = name;
        this.shortName = shortName;
        this.imageIdent = imageIdent;
        this.widthIdent = widthIdent;
        this.heightIdent = heightIdent;
        this.imageDog = imageDog;
        this.widthDog = widthDog;
        this.heightDog = heightDog;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("shortName")
    public String getShortName() {
        return shortName;
    }

    @JsonProperty("imageIdent")
    public String getImageIdent() {
        return imageIdent;
    }

    @JsonProperty("widthIdent")
    public Integer getWidthIdent() {
        return widthIdent;
    }

    @JsonProperty("heightIdent")
    public Integer getHeightIdent() {
        return heightIdent;
    }

    @JsonProperty("imageDog")
    public String getImageDog() {
        return imageDog;
    }

    @JsonProperty("widthDog")
    public Integer getWidthDog() {
        return widthDog;
    }

    @JsonProperty("heightDog")
    public Integer getHeightDog() {
        return heightDog;
    }
}
