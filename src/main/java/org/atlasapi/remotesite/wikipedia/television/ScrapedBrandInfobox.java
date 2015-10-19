package org.atlasapi.remotesite.wikipedia.television;

import org.joda.time.LocalDate;

import com.google.common.base.Optional;

public class ScrapedBrandInfobox {
    public String title;
    public String creator;
    public String episodeListLinkTarget;
    public Optional<LocalDate> firstAired = Optional.absent();
    public Optional<LocalDate> lastAired = Optional.absent();
    public String imdbID;
    public String image;

    @Override
    public String toString() {
        return "BrandInfoboxScraper.Result{" + "title=" + title + ", creator=" + creator + ", episodeListLinkTarget=" + episodeListLinkTarget + ", firstAired=" + firstAired + ", lastAired=" + lastAired +", image=" + image + '}';
    }
}