package org.atlasapi.remotesite.amazon;

import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.Duration;


public class AmazonItem {
    
    // TODO Trim out the unnecessary guff fields
    private final float amazonRating;
    private final Integer amazonRatingsCount;
    private final String asin;
    private final Boolean isTrident;
    private final ContentType contentType;
    private final Set<String> directors;
    private final Integer episodeNumber;
    // TODO how to parse genres
    private final Set<AmazonGenre> genres;
    private final String largeImageUrl;
    private final Quality quality;
    private final Boolean isPreOrder;
    private final Boolean isRental;
    private final Boolean isSeasonPass;
    private final Boolean isStreamable;
    private final String synopsis;
    private final String rating;
    private final String price;
    @JsonSerialize(using=DateTimeSerializer.class)
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
    private final String unboxSdPurchasePrice;
    private final String unboxSdPurchaseUrl;
    private final String unboxHdPurchasePrice;
    private final String unboxHdPurchaseUrl;
    private final String unboxSdRentalPrice;
    private final String unboxSdRentalUrl;
    private final String unboxHdRentalPrice;
    private final String unboxHdRentalUrl;
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static boolean isBrand(AmazonItem item) {
        return item.getContentType().equals(ContentType.TVSERIES);
    }

    public static boolean isSeries(AmazonItem item) {
        return item.getContentType().equals(ContentType.TVSEASON);
    }

    public static boolean isEpisode(AmazonItem item) {
        return item.getContentType().equals(ContentType.TVEPISODE);
    }

    public static boolean isFilm(AmazonItem item) {
        return item.getContentType().equals(ContentType.MOVIE);
    }
    
    private AmazonItem(Builder builder) {
        this.amazonRating = builder.amazonRating;
        this.amazonRatingsCount = builder.amazonRatingsCount;
        this.asin = builder.asin;
        this.contentType = builder.contentType;
        this.directors = builder.directors;
        this.episodeNumber = builder.episodeNumber;
        this.unboxSdPurchasePrice = builder.unboxSdPurchasePrice;
        this.unboxSdPurchaseUrl = builder.unboxSdPurchaseUrl;
        this.unboxHdPurchasePrice = builder.unboxHdPurchasePrice;
        this.unboxHdPurchaseUrl = builder.unboxHdPurchaseUrl;
        this.unboxSdRentalPrice = builder.unboxSdRentalPrice;
        this.unboxSdRentalUrl = builder.unboxSdRentalUrl;
        this.unboxHdRentalPrice = builder.unboxHdRentalPrice;
        this.unboxHdRentalUrl = builder.unboxHdRentalUrl;
        this.isTrident = builder.isTrident;
        this.starring = ImmutableSet.copyOf(builder.starring);
        this.genres = ImmutableSet.copyOf(builder.genres);
        this.largeImageUrl = builder.largeImageUrl;
        this.quality = builder.quality;
        this.isPreOrder = builder.isPreOrder;
        this.isRental = builder.isRental;
        this.isSeasonPass = builder.isSeasonPass;
        this.isStreamable = builder.isStreamable;
        this.synopsis = builder.synopsis;
        this.rating = builder.rating;
        this.price = builder.price;
        this.releaseDate = builder.releaseDate;
        this.duration = builder.duration;
        this.seasonAsin = builder.seasonAsin;
        this.seasonNumber = builder.seasonNumber;
        this.seriesAsin = builder.seriesAsin;
        this.seriesTitle = builder.seriesTitle;
        this.studio = builder.studio;
        this.tConst = builder.tConst;
        this.title = builder.title;
        this.isTivoEnabled = builder.isTivoEnabled;
        this.url = builder.url;
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

    public Set<String> getDirectors() {
        return directors;
    }
    
    public Integer getEpisodeNumber() {
        return episodeNumber;
    }
    
    public Set<AmazonGenre> getGenres() {
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
    
    public Boolean isTrident() {
        return isTrident;
    }
    
    public String getUrl() {
        return url;
    }
    
    public String getUnboxSdPurchasePrice() {
        return unboxSdPurchasePrice;
    }
    
    public String getUnboxSdPurchaseUrl() {
        return unboxSdPurchaseUrl;
    }
    
    public String getUnboxHdPurchasePrice() {
        return unboxHdPurchasePrice;
    }
    
    public String getUnboxHdPurchaseUrl() {
        return unboxHdPurchaseUrl;
    }
    

    public String getUnboxSdRentalPrice() {
        return unboxSdRentalPrice;
    }

    
    public String getUnboxSdRentalUrl() {
        return unboxSdRentalUrl;
    }

    
    public String getUnboxHdRentalPrice() {
        return unboxHdRentalPrice;
    }

    
    public String getUnboxHdRentalUrl() {
        return unboxHdRentalUrl;
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
        
        if (that instanceof AmazonItem) {
            AmazonItem other = (AmazonItem) that;
            return this.asin.equals(other.asin);
        }
        
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(AmazonItem.class)
                .add("amazonRating", amazonRating)
                .add("amazonRatingsCount", amazonRatingsCount)
                .add("asin", asin)
                .add("contentType", contentType)
                .add("directors", directors)
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
        private Set<String> directors = Sets.newHashSet();
        private Integer episodeNumber;
        private Set<AmazonGenre> genres = Sets.newHashSet();
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
        private String unboxSdPurchasePrice;
        private String unboxSdPurchaseUrl;
        private String unboxHdPurchasePrice;
        private String unboxHdPurchaseUrl;
        private String unboxSdRentalPrice;
        private String unboxSdRentalUrl;
        private String unboxHdRentalPrice;
        private String unboxHdRentalUrl;
        private Boolean isTrident;
        
        private Builder() {}
        
        public AmazonItem build() {
            return new AmazonItem(this);
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

        public Builder addDirectorRoles(Iterable<String> directors) {
            this.directors.addAll(Sets.newHashSet(directors));
            return this;
        }

        public Builder addDirectorRole(String director) {
            this.directors.add(director);
            return this;
        }
        
        public Builder withEpisodeNumber(Integer episodeNumber) {
            this.episodeNumber = episodeNumber;
            return this;
        }
        
        public Builder withGenres(Iterable<AmazonGenre> genres) {
            this.genres = Sets.newHashSet(genres);
            return this;
        }
        
        public Builder withGenre(AmazonGenre genre) {
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
        
        public Builder withIsTrident(Boolean isTrident) {
            this.isTrident = isTrident;
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

        public Builder addStarringRoles(Iterable<String> starring) {
            this.starring.addAll(Sets.newHashSet(starring));
            return this;
        }
        
        public Builder addStarringRole(String starring) {
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
        
        public Builder withUnboxSdPurchasePrice(String price) {
            this.unboxSdPurchasePrice = price;
            return this;
        }
        
        public Builder withUnboxSdPurchaseUrl(String url) {
            this.unboxSdPurchaseUrl = url;
            return this;
        }
        
        public Builder withUnboxHdPurchasePrice(String price) {
            this.unboxHdPurchasePrice = price;
            return this;
        }
        
        public Builder withUnboxHdPurchaseUrl(String url) {
            this.unboxHdPurchaseUrl = url;
            return this;
        }

        public Builder withUnboxSdRentalPrice(String price) {
            this.unboxSdRentalPrice = price;
            return this;
        
        }
        public Builder withUnboxSdRentalUrl(String url) {
            this.unboxSdRentalUrl = url;
            return this;
        }

        public Builder withUnboxHdRentalPrice(String price) {
            this.unboxHdRentalPrice = price;
            return this;
        }

        public Builder withUnboxHdRentalUrl(String url) {
            this.unboxHdRentalUrl = url;
            return this;
        }
    }
}
