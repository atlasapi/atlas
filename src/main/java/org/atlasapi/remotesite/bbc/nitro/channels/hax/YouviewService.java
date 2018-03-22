package org.atlasapi.remotesite.bbc.nitro.channels.hax;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class YouviewService {

    private final String name;
    private final String locator;
    private final List<LocatorWithRegions> locators;
    private final String sid;
    private final String shortName;
    private final String image;
    private final Integer width;
    private final Integer height;
    private final Set<String> targets;
    private final Boolean interactive;

    @JsonCreator
    static YouviewService create(
            @JsonProperty("name") String name,
            @JsonProperty("locator") String locator,
            @JsonProperty("locators") List<LocatorWithRegions> locators,
            @JsonProperty("sid") String sid,
            @JsonProperty("shortName") String shortName,
            @JsonProperty("image") String image,
            @JsonProperty("width") Integer width,
            @JsonProperty("height") Integer height,
            @JsonProperty("targets") Set<String> targets,
            @JsonProperty("interactive") Boolean interactive
    ) {
        return new YouviewService(
                name,
                locator,
                locators,
                sid,
                shortName,
                image,
                width,
                height,
                targets,
                interactive
        );
    }

    private YouviewService(
            String name,
            String locator,
            List<LocatorWithRegions> locators,
            String sid,
            String shortName,
            String image,
            Integer width,
            Integer height,
            Set<String> targets,
            Boolean interactive
    ) {
        this.name = name;
        this.locator = locator;
        this.locators = locators;
        this.sid = sid;
        this.shortName = shortName;
        this.image = image;
        this.width = width;
        this.height = height;
        this.targets = targets;
        this.interactive = interactive;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("locators")
    public List<LocatorWithRegions> getLocators() {
        return locators;
    }

    @JsonProperty("locator")
    public String getLocator() {
        return locator;
    }

    @JsonProperty("shortName")
    public String getShortName() {
        return shortName;
    }

    @JsonProperty("image")
    public String getImage() {
        return image;
    }

    @JsonProperty("width")
    public Integer getWidth() {
        return width;
    }

    @JsonProperty("height")
    public Integer getHeight() {
        return height;
    }

    @JsonProperty("targets")
    public Set<String> getTargets() {
        return targets;
    }

    @JsonProperty("interactive")
    public Boolean getInteractive() {
        return interactive;
    }

    @JsonProperty("sid")
    public String getSid() {
        return sid;
    }
}
