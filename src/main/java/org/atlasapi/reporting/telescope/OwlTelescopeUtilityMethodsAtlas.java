package org.atlasapi.reporting.telescope;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

public class OwlTelescopeUtilityMethodsAtlas {

    public static ImmutableList<Alias> getAliases(Set<org.atlasapi.media.entity.Alias> aliases) {
        return aliases.stream()
                .map(alias -> Alias.create(alias.getNamespace(), alias.getValue()))
                .collect(MoreCollectors.toImmutableList());
    }

    /**
     * Returns the appropriate {@link EntityType} based on the given object's class.
     * The closest parent will be used if there is not a one-to-one matching, though if that
     * happens you might want to update the enumeration (and this method), or bypass this method
     * alltogether and report to telescope directly the EntityType String that you need.
     */
    @Nullable
    public static <T extends Identified> EntityType getEntityTypeFor(T object) {

        if (object == null) {
            return null;
        }

        if (object instanceof Content) {
            if (object instanceof Container) {
                if (object instanceof Brand) {
                    return EntityType.BRAND;
                } else if (object instanceof Series) {
                    return EntityType.SERIES;
                } else {
                    return EntityType.CONTAINER;
                }
            } else if (object instanceof Item) {
                if (object instanceof Film) {
                    return EntityType.FILM;
                } else if (object instanceof Clip) {
                    return EntityType.CLIP;
                } else if (object instanceof Episode) {
                    return EntityType.EPISODE;
                } else if (object instanceof Song) {
                    return EntityType.SONG;
                } else {
                    return EntityType.ITEM;
                }
            } else {
                return EntityType.CONTENT;
            }
        } else if (object instanceof Channel) {
            return EntityType.CHANNEL;
        } else if (object instanceof ChannelGroup) {
            if (object instanceof Region) {
                return EntityType.REGION;
            } else if (object instanceof Platform) {
                return EntityType.PLATFORM;
            } else {
                return EntityType.CHANNEL_GROUP;
            }
        } else if (object instanceof Event) {
            return EntityType.EVENT;
        } else if (object instanceof Version) {
            return EntityType.VERSION;
        } else if (object instanceof Broadcast) {
            return EntityType.BROADCAST;
        }

        return null;
    }
}
