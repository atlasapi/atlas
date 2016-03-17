package org.atlasapi.remotesite.opta.events;

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.common.base.Optional;


/**
 * Wraps the three parameters that uniquely define a sports feed from Opta.
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
public class OptaSportConfiguration {
    
    private final String feedType;
    private final String competition;
    private final String seasonId;
    private Optional<String> prefixToStripFromId;
    
    public static Builder builder() {
        return new Builder();
    }
    
    private OptaSportConfiguration(String feedType, String competition, 
            String seasonId, String prefixToStripFromOptaId) {
        this.feedType = checkNotNull(feedType);
        this.competition = checkNotNull(competition);
        this.seasonId = checkNotNull(seasonId);
        this.prefixToStripFromId = Optional.fromNullable(prefixToStripFromOptaId);
    }
    
    public String feedType() {
        return feedType;
    }
    
    public String competition() {
        return competition;
    }
    
    public String seasonId() {
        return seasonId;
    }
    
    public Optional<String> prefixToStripFromId() {
        return prefixToStripFromId;
    }

    public static class Builder {
        
        private String feedType;
        private String competition;
        private String seasonId;
        private String prefixToStripFromId;
        
        public OptaSportConfiguration build() {
            return new OptaSportConfiguration(feedType, competition, seasonId, prefixToStripFromId);
        }
        
        private Builder() { }
        
        public Builder withFeedType(String feedType) {
            this.feedType = feedType;
            return this;
        }
        
        public Builder withCompetition(String competition) {
            this.competition = competition;
            return this;
        }
        
        public Builder withSeasonId(String seasonId) {
            this.seasonId = seasonId;
            return this;
        }

        public Builder withPrefixToStripFromId(String prefixToStripFromId) {
            this.prefixToStripFromId = prefixToStripFromId;
            return this;
        }
    }
}
