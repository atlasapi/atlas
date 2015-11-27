package org.atlasapi.remotesite.btvod;

import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class BtVodCollection {

    private final DateTime created;
    private final String description;
    private final String longDescription;
    private final Set<Image> images;
    private final String image;

    public BtVodCollection(DateTime created, String description, String longDescription,
            Set<Image> images) {
        this.created = created;
        this.description = description;
        this.longDescription = longDescription;
        this.images = ImmutableSet.copyOf(images);
        if (this.images.size() > 0) {
            this.image = Iterables.get(this.images, 0).getCanonicalUri();
        }
        else {
            this.image = null;
        }
    }

    public DateTime getCreated() {
        return created;
    }

    public String getDescription() {
        return description;
    }

    public String getLongDescription() {
        return longDescription;
    }

    public Set<Image> getImages() {
        return images;
    }

    public String getImage() {
        return image;
    }
}
