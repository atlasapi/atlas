package org.atlasapi.remotesite.wikipedia.wikiparsers;

public interface ArticleFetcher {
    public static class FetchFailedException extends Exception {}
    
    Article fetchArticle(String title) throws FetchFailedException;
}
