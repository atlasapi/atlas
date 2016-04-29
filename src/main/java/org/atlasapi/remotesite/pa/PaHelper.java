package org.atlasapi.remotesite.pa;

import org.atlasapi.media.entity.Alias;

public class PaHelper {
    
    private static final String PA_BASE_URL = "http://pressassociation.com/";
    private static final String PA_BASE_ALIAS = "pa:";
    private static final String RT_FILM_ALIAS = "rt:filmid";
    
    public static String getFilmUri(String id) {
        //previously film ids were generated from rtfilmnumber, now we will be using progId
        //since they have different numberspace, we will save films under /episode/progId
        return getEpisodeUri(id);
    }
    
    public static Alias getProgIdAlias(String id) {
        // Inconsistent namespace with others, but this is
        // properly namespaced
        return new Alias("gb:pressassociation:prod:prog_id", id);
    }
    
    public static Alias getFilmAlias(String id) {
        return new Alias(PA_BASE_ALIAS + "film", id);
    }
    
    public static Alias getRtFilmAlias(String id) {
        return new Alias(RT_FILM_ALIAS, id);
    }
    
    public static String getEpisodeUri(String id) {
        return PA_BASE_URL + "episodes/" + id;
    }

    public static String getFilmRtAlias(String rtFilmNumber) {
        return PA_BASE_URL + "films/" + rtFilmNumber;
    }

    public static Alias getEpisodeAlias(String id) {
        return new Alias(PA_BASE_ALIAS + "episode", id);
    }
    
    public static String getAlias(String id) {
        return PA_BASE_URL + id;
    }

    public static Alias getAliasItem(String id) {
        return new Alias(PA_BASE_ALIAS, id);
    }
    
    public static String getBrandUri(String id) {
        return PA_BASE_URL + "brands/" + id;
    }
    
    public static Alias getBrandAlias(String id) {
        return new Alias(PA_BASE_ALIAS + "brand", id);
    }
    
    public static String getSeriesUri(String id, String seriesNumber) {
        return PA_BASE_URL + "series/" + id + "-" + seriesNumber;
    }
    
    public static Alias getSeriesAlias(String id, String seriesNumber) {
        return new Alias(PA_BASE_ALIAS + "series", id + "-" + seriesNumber);
    }
    
    public static String getEpisodeCurie(String id) {
        return "pa:e-" + id;
    }

    
    public static String getBroadcastId(String slotId) {
        return "pa:" + slotId;
    }
}
