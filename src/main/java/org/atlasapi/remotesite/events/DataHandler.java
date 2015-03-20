package org.atlasapi.remotesite.events;


public interface DataHandler<S, T, M> {

    void handleTeam(T team, S sport);
    void handleMatch(M match, S sport);
}
