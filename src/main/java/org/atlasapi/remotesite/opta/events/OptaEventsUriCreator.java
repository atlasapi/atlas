package org.atlasapi.remotesite.opta.events;

import org.atlasapi.remotesite.events.EventsUriCreator;


public class OptaEventsUriCreator implements EventsUriCreator {

    private static final String EVENT_URI_BASE = "http://optasports.com/events/";
    private static final String TEAM_URI_BASE = "http://optasports.com/teams/";
    
    public OptaEventsUriCreator() { }

    @Override
    public String createEventUri(String id) {
        return EVENT_URI_BASE + id;
    }

    @Override
    public String createTeamUri(String id) {
        return TEAM_URI_BASE + normalizeTeamId(id);
    }
    
    /**
     * Previously we received numeric IDs in the feed. However, when
     * we switched to the opta API from a file, the IDs were prefixed
     * with a leading "t". So as to reference the previously-created
     * teams, we'll strip the leading "t", if present.
     */
    private String normalizeTeamId(String id) {
        if (id.startsWith("t")) {
            return id.substring(1);
        } else {
            return id;
        }
    }

}
