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
    private final String username;
    private final String password;
    private Optional<String> prefixToStripFromId;
    
    public static Builder builder() {
        return new Builder();
    }
    
    private OptaSportConfiguration(String feedType, String competition, 
            String seasonId, String prefixToStripFromOptaId, String username, String password) {
        this.feedType = checkNotNull(feedType);
        this.competition = checkNotNull(competition);
        this.seasonId = checkNotNull(seasonId);
        this.prefixToStripFromId = Optional.fromNullable(prefixToStripFromOptaId);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
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
    };

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public static class Builder {
        
        private String feedType;
        private String competition;
        private String seasonId;
        private String prefixToStripFromId;
        private String username;
        private String password;
        
        public OptaSportConfiguration build() {
            return new OptaSportConfiguration(feedType, competition, seasonId, prefixToStripFromId,
                    username, password);
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

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

            public Builder withPassword(String password) {
            this.password = password;
            return this;
        }
    }
}
