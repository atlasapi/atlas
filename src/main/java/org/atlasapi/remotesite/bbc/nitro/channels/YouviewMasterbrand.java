package org.atlasapi.remotesite.bbc.nitro.channels;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class YouviewMasterbrand {

    @JsonProperty
    private String name;
    @JsonProperty
    private String shortName;
    @JsonProperty
    private String image;
    @JsonProperty
    private Integer width;
    @JsonProperty
    private Integer height;

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

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

}
