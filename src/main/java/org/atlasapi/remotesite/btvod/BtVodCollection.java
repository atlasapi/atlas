package org.atlasapi.remotesite.btvod;

import java.util.Objects;
import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class BtVodCollection {

    private final String guid;
    private final DateTime created;
    private final String description;
    private final String longDescription;
    private final Set<Image> images;
    private final String image;

    public BtVodCollection(String guid, DateTime created, String description,
            String longDescription, Set<Image> images) {
        this.guid = guid;
        this.created = created;
        this.description = description;
        this.longDescription = longDescription;
        this.images = images != null ? ImmutableSet.copyOf(images) : ImmutableSet.<Image>of();
        if (this.images.size() > 0) {
            this.image = Iterables.get(this.images, 0).getCanonicalUri();
        }
        else {
            this.image = null;
        }
    }

    public String getGuid() {
        return guid;
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BtVodCollection that = (BtVodCollection) o;
        return Objects.equals(guid, that.guid) &&
                Objects.equals(created, that.created) &&
                Objects.equals(description, that.description) &&
                Objects.equals(longDescription, that.longDescription) &&
                Objects.equals(images, that.images) &&
                Objects.equals(image, that.image);
    }

    @Override
    public int hashCode() {
        return Objects.hash(guid, created, description, longDescription, images, image);
    }
}
