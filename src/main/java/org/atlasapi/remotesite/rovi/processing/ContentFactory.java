package org.atlasapi.remotesite.rovi.processing;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.rovi.model.RoviShowType;

import com.google.common.collect.Maps;


public class ContentFactory {
    
    private static final Map<RoviShowType, Class<? extends Content>> showTypeToClass = Maps.newHashMap();
    
    static {
        showTypeToClass.put(RoviShowType.MOVIE, Film.class);
        showTypeToClass.put(RoviShowType.SERIES_EPISODE, Episode.class);
        showTypeToClass.put(RoviShowType.SERIES_MASTER, Brand.class);
        showTypeToClass.put(RoviShowType.OTHER, Item.class);
    }
    
    public static Content createContent(RoviShowType showType) {
        switch(showType) {
            case MOVIE:
                return new Film();
            case SERIES_EPISODE:
                return new Episode();
            case SERIES_MASTER:
                return new Brand();
            case OTHER:
                return new Item();
            default:
                throw new IllegalArgumentException("Unexpected show type: " + showType);
        }
    }

    public static boolean hasCorrectType(Content content, RoviShowType showType) {
        checkShowType(showType);
        
        Class<? extends Content> clazz = showTypeToClass.get(showType);
        return content.getClass().equals(clazz);
    }

    private static void checkShowType(RoviShowType showType) {
        checkArgument(showTypeToClass.containsKey(showType), "Unexpected show type: " + showType);
    }
}
