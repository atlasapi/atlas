package org.atlasapi.equiv.update.metadata;

import java.util.ArrayList;
import java.util.List;


public class EquivToTelescopeResults {

    private String contentId;
    private String publisher;
    private List<EquivToTelescopeResult> results;

    private EquivToTelescopeResults(String contentId, String publisher) {
        this.contentId = contentId;
        this.publisher = publisher;
        results = new ArrayList<>();
    }

    public static EquivToTelescopeResults create(String contentId, String publisher) {
        return new EquivToTelescopeResults(contentId, publisher);
    }

    public void addResult(EquivToTelescopeResult result) {
        results.add(result);
    }

    public String getContentId() {
        return contentId;
    }

    public String getPublisher() {
        return publisher;
    }

    public List<EquivToTelescopeResult> getResults() {
        return results;
    }
}