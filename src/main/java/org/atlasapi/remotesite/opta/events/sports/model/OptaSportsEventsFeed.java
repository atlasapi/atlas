package org.atlasapi.remotesite.opta.events.sports.model;

import static com.google.api.client.util.Preconditions.checkNotNull;



public class OptaSportsEventsFeed {

    private final OptaDocument feed;
    
    public OptaSportsEventsFeed(OptaDocument feed) {
        this.feed = checkNotNull(feed);
    }

    public OptaDocument feed() {
        return feed;
    }
    
    public static class OptaDocument {
        
        private final OptaSportsDocument document;
        
        public OptaDocument(OptaSportsDocument document) {
            this.document = checkNotNull(document);
        }

        public OptaSportsDocument document() {
            return document;
        }
    }
}
