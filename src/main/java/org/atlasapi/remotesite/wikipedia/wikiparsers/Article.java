package org.atlasapi.remotesite.wikipedia.wikiparsers;

import org.joda.time.DateTime;

public abstract class Article {

    public abstract DateTime getLastModified();

    public abstract String getMediaWikiSource();

    public abstract String getTitle();

    public String getUrl() {
        return urlFromTitle(getTitle());
    }
    
    /**
     * Calculates the English Wikipedia URL for a given article title.
     */
    public static String urlFromTitle(String title) {
        String safeTitle = title.replaceAll(" ", "_")
                .replaceAll("\"", "%22").replaceAll("'", "%27")
                .replaceAll(",", "%2C").replaceAll(";", "%3B")
                .replaceAll("<", "%3C").replaceAll(">", "%3E")
                .replaceAll("\\[", "%5B").replaceAll("]", "%5D")
                .replaceAll("\\?", "%3F");
        // see: http://en.wikipedia.org/wiki/Help:URL#URLs_of_Wikipedia_pages
        return "http://en.wikipedia.org/wiki/" + safeTitle;
    }

}
