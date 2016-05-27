package org.atlasapi.remotesite.bbc.nitro.channels;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class YouviewService {

    @JsonProperty
    private String name;
    @JsonProperty
    private String locator;
    @JsonProperty
    private String shortName;
    @JsonProperty
    private String image;
    @JsonProperty
    private Integer width;
    @JsonProperty
    private Integer height;
    @JsonProperty
    private Set<String> targets;
    @JsonProperty
    private Boolean interactive;

    public YouviewService() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocator() {
        return locator;
    }

    public void setLocator(String locator) {
        this.locator = locator;
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

    public Set<String> getTargets() {
        return targets;
    }

    public void setTargets(Set<String> targets) {
        this.targets = targets;
    }

    public Boolean getInteractive() {
        return interactive;
    }

    public void setInteractive(Boolean interactive) {
        this.interactive = interactive;
    }
}
