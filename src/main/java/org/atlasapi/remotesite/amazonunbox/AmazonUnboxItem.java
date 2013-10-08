package org.atlasapi.remotesite.amazonunbox;

import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


public class AmazonUnboxItem {
    
    // TODO Trim out the unnecessary guff fields
    private final float amazonRating;
    private final Integer amazonRatingsCount;
    private final String asin;
    private final ContentType contentType;
    private final String director;
    private final Integer episodeNumber;
    // TODO how to parse genres
    private final Set<AmazonUnboxGenre> genres;
    private final String largeImageUrl;
    private final Quality quality;
    private final Boolean isPreOrder;
    private final Boolean isRental;
    private final Boolean isSeasonPass;
    private final Boolean isStreamable;
    private final String synopsis;
    private final String rating;
    private final String price;
    private final DateTime releaseDate;
    private final Duration duration;
    private final Set<String> starring;
    private final String seasonAsin;
    private final Integer seasonNumber;
    private final String seriesAsin;
    private final String seriesTitle;
    private final String studio;
    private final String tConst;
    private final String title;
    private final Boolean isTivoEnabled;
    private final String url;
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static boolean isBrand(AmazonUnboxItem item) {
        return item.getContentType().equals(ContentType.TVSERIES);
    }

    public static boolean isSeries(AmazonUnboxItem item) {
        return item.getContentType().equals(ContentType.TVSEASON);
    }

    public static boolean isEpisode(AmazonUnboxItem item) {
        return item.getContentType().equals(ContentType.TVEPISODE);
    }

    public static boolean isFilm(AmazonUnboxItem item) {
        return item.getContentType().equals(ContentType.MOVIE);
    }
    
    private AmazonUnboxItem(float amazonRating, Integer amazonRatingsCount, String asin, ContentType contentType, String director, 
            Integer episodeNumber, Set<AmazonUnboxGenre> genres, String largeImageUrl, Quality quality, Boolean isPreOrder, 
            Boolean isRental, Boolean isSeasonPass, Boolean isStreamable, String synopsis, String rating, String price, 
            DateTime releaseDate, Duration duration, Set<String> starring, String seasonAsin, Integer seasonNumber, 
            String seriesAsin, String seriesTitle, String studio, String tConst, String title, Boolean isTivoEnabled, String url) {
        this.amazonRating = amazonRating;
        this.amazonRatingsCount = amazonRatingsCount;
        this.asin = asin;
        this.contentType = contentType;
        this.director = director;
        this.episodeNumber = episodeNumber;
        this.starring = ImmutableSet.copyOf(starring);
        this.genres = ImmutableSet.copyOf(genres);
        this.largeImageUrl = largeImageUrl;
        this.quality = quality;
        this.isPreOrder = isPreOrder;
        this.isRental = isRental;
        this.isSeasonPass = isSeasonPass;
        this.isStreamable = isStreamable;
        this.synopsis = synopsis;
        this.rating = rating;
        this.price = price;
        this.releaseDate = releaseDate;
        this.duration = duration;
        this.seasonAsin = seasonAsin;
        this.seasonNumber = seasonNumber;
        this.seriesAsin = seriesAsin;
        this.seriesTitle = seriesTitle;
        this.studio = studio;
        this.tConst = tConst;
        this.title = title;
        this.isTivoEnabled = isTivoEnabled;
        this.url = url;
    }
    
    public float getAmazonRating() {
        return amazonRating;
    }
    
    public Integer getAmazonRatingsCount() {
        return amazonRatingsCount;
    }
    
    public String getAsin() {
        return asin;
    }
    
    public ContentType getContentType() {
        return contentType;
    }
    
    public String getDirector() {
        return director;
    }
    
    public Integer getEpisodeNumber() {
        return episodeNumber;
    }
    
    public Set<AmazonUnboxGenre> getGenres() {
        return genres;
    }
    
    public String getLargeImageUrl() {
        return largeImageUrl;
    }
    
    public Quality getQuality() {
        return quality;
    }
    
    public Boolean isPreOrder() {
        return isPreOrder;
    }
    
    public Boolean isRental() {
        return isRental;
    }
    
    public Boolean isSeasonPass() {
        return isSeasonPass;
    }
    
    public Boolean isStreamable() {
        return isStreamable;
    }
    
    public String getSynopsis() {
        return synopsis;
    }
    
    public String getRating() {
        return rating;
    }
    
    public String getPrice() {
        return price;
    }
    
    public DateTime getReleaseDate() {
        return releaseDate;
    }
    
    public Duration getDuration() {
        return duration;
    }
    
    public Set<String> getStarring() {
        return starring;
    }
    
    public String getSeasonAsin() {
        return seasonAsin;
    }
    
    public Integer getSeasonNumber() {
        return seasonNumber;
    }
    
    public String getSeriesAsin() {
        return seriesAsin;
    }
    
    public String getSeriesTitle() {
        return seriesTitle;
    }
    
    public String getStudio() {
        return studio;
    }
    
    public String getTConst() {
        return tConst;
    }
    
    public String getTitle() {
        return title;
    }
    
    public Boolean isTivoEnabled() {
        return isTivoEnabled;
    }
    
    public String getUrl() {
        return url;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(asin);
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) { 
            return true;
        }
        
        if (that instanceof AmazonUnboxItem) {
            AmazonUnboxItem other = (AmazonUnboxItem) that;
            return this.asin.equals(other.asin);
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(AmazonUnboxItem.class)
                .add("amazonRating", amazonRating)
        .add("amazonRatingsCount", amazonRatingsCount)
        .add("asin", asin)
        .add("contentType", contentType)
        .add("director", director)
        .add("episodeNumber", episodeNumber)
        .add("genres", genres)
        .add("largeImageUrl", largeImageUrl)
        .add("quality", quality)
        .add("isPreOrder", isPreOrder)
        .add("isRental", isRental)
        .add("isSeasonPass", isSeasonPass)
        .add("isStreamable", isStreamable)
        .add("synopsis", synopsis)
        .add("rating", rating)
        .add("price", price)
        .add("releaseDate", releaseDate)
        .add("duration", duration)
        .add("starring", starring)
        .add("seasonAsin", seasonAsin)
        .add("seasonNumber", seasonNumber)
        .add("seriesAsin", seriesAsin)
        .add("seriesTitle", seriesTitle)
        .add("studio", studio)
        .add("tConst", tConst)
        .add("title", title)
        .add("isTivoEnabled", isTivoEnabled)
        .add("url", url)
        .toString();
    }
    
    public static class Builder {
        
     // TODO Trim out the unnecessary guff fields
        private float amazonRating;
        private Integer amazonRatingsCount;
        private String asin;
        private ContentType contentType;
        private String director;
        private Integer episodeNumber;
        private Set<AmazonUnboxGenre> genres = Sets.newHashSet();
        private String largeImageUrl;
        private Quality quality;
        private Boolean isPreOrder;
        private Boolean isRental;
        private Boolean isSeasonPass;
        private Boolean isStreamable;
        private String synopsis;
        private String rating;
        private String price;
        private DateTime releaseDate;
        private Duration duration;
        private Set<String> starring = Sets.newHashSet();
        private String seasonAsin;
        private Integer seasonNumber;
        private String seriesAsin;
        private String seriesTitle;
        //TODO figure out how to parse these
        //private List<RelatedProduct> relatedProducts;
        private String studio;
        private String tConst;
        private String title;
        private Boolean isTivoEnabled;
        private String url;
        
        private Builder() {}
        
        public AmazonUnboxItem build() {
            return new AmazonUnboxItem(amazonRating, amazonRatingsCount, asin, contentType, director, 
                    episodeNumber, genres, largeImageUrl, quality, isPreOrder, isRental, isSeasonPass, 
                    isStreamable, synopsis, rating, price, releaseDate, duration, starring, seasonAsin, 
                    seasonNumber, seriesAsin, seriesTitle, studio, tConst, title, isTivoEnabled, url);
        }
        
        public Builder withAmazonRating(float amazonRating) {
            this.amazonRating = amazonRating;
            return this;
        }
        
        public Builder withAmazonRatingsCount(Integer amazonRatingsCount) {
            this.amazonRatingsCount = amazonRatingsCount;
            return this;
        }
        
        public Builder withAsin(String asin) {
            this.asin = asin;
            return this;
        }
        
        public Builder withContentType(ContentType contentType) {
            this.contentType = contentType;
            return this;
        }
        
        public Builder withDirector(String director) {
            this.director = director;
            return this;
        }
        
        public Builder withEpisodeNumber(Integer episodeNumber) {
            this.episodeNumber = episodeNumber;
            return this;
        }
        
        public Builder withGenres(Iterable<AmazonUnboxGenre> genres) {
            this.genres = Sets.newHashSet(genres);
            return this;
        }
        
        public Builder withGenre(AmazonUnboxGenre genre) {
            this.genres.add(genre);
            return this;
        }
        
        public Builder withLargeImageUrl(String largeImageUrl) {
            this.largeImageUrl = largeImageUrl;
            return this;
        }
        
        public Builder withQuality(Quality quality) {
            this.quality = quality;
            return this;
        }
        
        public Builder withPreOrder(Boolean isPreOrder) {
            this.isPreOrder = isPreOrder;
            return this;
        }
        
        public Builder withRental(Boolean isRental) {
            this.isRental = isRental;
            return this;
        }
        
        public Builder withSeasonPass(Boolean isSeasonPass) {
            this.isSeasonPass = isSeasonPass;
            return this;
        }
        
        public Builder withStreamable(Boolean isStreamable) {
            this.isStreamable = isStreamable;
            return this;
        }
        
        public Builder withSynopsis(String synopsis) {
            this.synopsis = synopsis;
            return this;
        }
        
        public Builder withRating(String rating) {
            this.rating = rating;
            return this;
        }
        
        public Builder withPrice(String price) {
            this.price = price;
            return this;
        }
        
        public Builder withReleaseDate(DateTime releaseDate) {
            this.releaseDate = releaseDate;
            return this;
        }
        
        public Builder withDuration(Duration duration) {
            this.duration = duration;
            return this;
        }
        
        public Builder withStarringRoles(Iterable<String> starring) {
            this.starring = Sets.newHashSet(starring);
            return this;
        }
        
        public Builder withStarring(String starring) {
            this.starring.add(starring);
            return this;
        }
        
        public Builder withSeasonAsin(String seasonAsin) {
            this.seasonAsin = seasonAsin;
            return this;
        }
        
        public Builder withSeasonNumber(Integer seasonNumber) {
            this.seasonNumber = seasonNumber;
            return this;
        }
        
        public Builder withSeriesAsin(String seriesAsin) {
            this.seriesAsin = seriesAsin;
            return this;
        }
        
        public Builder withSeriesTitle(String seriesTitle) {
            this.seriesTitle = seriesTitle;
            return this;
        }
        
        public Builder withStudio(String studio) {
            this.studio = studio;
            return this;
        }
        
        public Builder withTConst(String tConst) {
            this.tConst = tConst;
            return this;
        }
        
        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }
        
        public Builder withTivoEnabled(Boolean isTivoEnabled) {
            this.isTivoEnabled = isTivoEnabled;
            return this;
        }
        
        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }
    }
}
