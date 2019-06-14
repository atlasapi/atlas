package org.atlasapi.equiv.update.metadata;

import java.util.ArrayList;
import java.util.List;


public class EquivToTelescopeResult {

    private String contentId;
    private String publisher;
    private List<EquivToTelescopeComponent> generators;
    private List<EquivToTelescopeComponent> scorers;
    private List<EquivToTelescopeComponent> combiners;
    private List<EquivToTelescopeComponent> filters;
    private List<EquivToTelescopeComponent> extractors;

    private EquivToTelescopeResult(String contentId, String publisher) {
        this.contentId = contentId;
        this.publisher = publisher;
        generators = new ArrayList<>();
        scorers = new ArrayList<>();
        combiners = new ArrayList<>();
        filters = new ArrayList<>();
        extractors = new ArrayList<>();
    }

    public static EquivToTelescopeResult create(String contentId, String publisher) {
        return new EquivToTelescopeResult(contentId, publisher);
    }

    public void addGeneratorResult(EquivToTelescopeComponent generator) {
        generators.add(generator);
    }

    public void addScorerResult(EquivToTelescopeComponent scorer) {
        scorers.add(scorer);
    }

    public void addCombinerResult(EquivToTelescopeComponent combiner) {
        combiners.add(combiner);
    }

    public void addFilterResult(EquivToTelescopeComponent filter) {
        filters.add(filter);
    }

    public void addExtractorResult(EquivToTelescopeComponent extractor) {
        extractors.add(extractor);
    }

    public String getContentId() {
        return contentId;
    }

    public String getPublisher() {
        return publisher;
    }

    public List<EquivToTelescopeComponent> getGenerators() {
        return generators;
    }

    public List<EquivToTelescopeComponent> getScorers() {
        return scorers;
    }

    public List<EquivToTelescopeComponent> getCombiners() {
        return combiners;
    }

    public List<EquivToTelescopeComponent> getFilters() {
        return filters;
    }

    public List<EquivToTelescopeComponent> getExtractors() {
        return extractors;
    }
}