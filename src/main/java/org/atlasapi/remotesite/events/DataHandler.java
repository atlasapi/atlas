package org.atlasapi.remotesite.events;


public interface DataHandler<S, T, M> {

    void handle(T team);
    void handle(M match, S sport);
}