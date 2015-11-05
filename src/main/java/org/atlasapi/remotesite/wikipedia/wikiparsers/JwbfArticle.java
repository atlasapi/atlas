package org.atlasapi.remotesite.wikipedia.wikiparsers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.DateTime;

public class JwbfArticle extends Article {
    private final net.sourceforge.jwbf.core.contentRep.Article jwbfArticle;
    
    public JwbfArticle(net.sourceforge.jwbf.core.contentRep.Article jwbfArticle)
    {
        this.jwbfArticle = checkNotNull(jwbfArticle);
    }
    
    @Override
    public String getMediaWikiSource() {
        return jwbfArticle.getText();
    }
    
    @Override
    public String getTitle() {
        return jwbfArticle.getTitle();
    }
    
    @Override
    public DateTime getLastModified() {
        return new DateTime(jwbfArticle.getEditTimestamp());
    }
}
