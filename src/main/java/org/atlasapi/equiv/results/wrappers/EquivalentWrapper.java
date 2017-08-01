package org.atlasapi.equiv.results.wrappers;

import java.util.List;

public class EquivalentWrapper {

    private String uri;
    private String title;
    private String publisher;
    private double combined;
    private List<ScoreWrapper> scores;

    private EquivalentWrapper(Builder builder) {
        this.uri = builder.uri;
        this.title = builder.title;
        this.publisher = builder.publisher;
        this.combined = builder.combined;
        this.scores = builder.scores;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public double getCombined() {
        return combined;
    }

    public void setCombined(double combined) {
        this.combined = combined;
    }

    public List<ScoreWrapper> getScores() {
        return scores;
    }

    public void setScores(List<ScoreWrapper> scores) {
        this.scores = scores;
    }

    public static class Builder {

        private String uri;
        private String title;
        private String publisher;
        private double combined;
        private List<ScoreWrapper> scores;

        private Builder() {}

        public Builder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withPublisher(String publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withCombined(double combined) {
            this.combined = combined;
            return this;
        }

        public Builder withScores(List<ScoreWrapper> scores) {
            this.scores = scores;
            return this;
        }

        public EquivalentWrapper build() {
            return new EquivalentWrapper(this);
        }
    }
}
