package org.atlasapi.remotesite.btvod.model;

import java.util.List;

public class BtVodEntry {

    private String guid;
    private String title;
    private String description;
    private String plproduct$longDescription;
    private List<BtVodPlproduct$scopes> plproduct$scopes;
    private List<BtVodPlproduct$productTag> plproduct$productTags;

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
}
