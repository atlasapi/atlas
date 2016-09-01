package org.atlasapi.remotesite.bbc.nitro.channels;

import com.fasterxml.jackson.annotation.JsonProperty;

public class YouviewMasterbrand {

    @JsonProperty
    private String name;
    @JsonProperty
    private String shortName;
    @JsonProperty
    private String imageIdent;
    @JsonProperty
    private Integer widthIdent;
    @JsonProperty
    private Integer heightIdent;
    @JsonProperty
    private String imageDog;
    @JsonProperty
    private Integer widthDog;
    @JsonProperty
    private Integer heightDog;

    public YouviewMasterbrand() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public void setImageIdent(String imageIdent) {
        this.imageIdent = imageIdent;
    }

    public void setWidthIdent(Integer widthIdent) {
        this.widthIdent = widthIdent;
    }

    public void setHeightIdent(Integer heightIdent) {
        this.heightIdent = heightIdent;
    }

    public void setImageDog(String imageDog) {
        this.imageDog = imageDog;
    }

    public void setWidthDog(Integer widthDog) {
        this.widthDog = widthDog;
    }

    public void setHeightDog(Integer heightDog) {
        this.heightDog = heightDog;
    }

    public String getImageIdent() {
        return imageIdent;
    }

    public Integer getWidthIdent() {
        return widthIdent;
    }

    public Integer getHeightIdent() {
        return heightIdent;
    }

    public String getImageDog() {
        return imageDog;
    }

    public Integer getWidthDog() {
        return widthDog;
    }

    public Integer getHeightDog() {
        return heightDog;
    }
}
