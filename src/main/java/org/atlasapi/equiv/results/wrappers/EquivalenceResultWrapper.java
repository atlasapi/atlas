package org.atlasapi.equiv.results.wrappers;

import java.util.List;
import java.util.Set;

public class EquivalenceResultWrapper {

    private String uri;
    private String title;
    private Set<String> strong;
    private List<EquivalentWrapper> equiv;
    private String timestamp;
    private List<Object> description;

    private EquivalenceResultWrapper(Builder builder) {
        this.uri = builder.uri;
        this.title = builder.title;
        this.strong = builder.strong;
        this.equiv = builder.equiv;
        this.timestamp = builder.timestamp;
        this.description = builder.description;
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

    public Set<String> getStrong() {
        return strong;
    }

    public void setStrong(Set<String> strong) {
        this.strong = strong;
    }

    public List<EquivalentWrapper> getEquiv() {
        return equiv;
    }

    public void setEquiv(List<EquivalentWrapper> equiv) {
        this.equiv = equiv;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<Object> getDescription() {
        return description;
    }

    public void setDescription(List<Object> description) {
        this.description = description;
    }

    public static class Builder {

        private String uri;
        private String title;
        private Set<String> strong;
        private List<EquivalentWrapper> equiv;
        private String timestamp;
        private List<Object> description;

        private Builder() {}

        public Builder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withStrong(Set<String> strong) {
            this.strong = strong;
            return this;
        }

        public Builder withEquivalentsWrapper(List<EquivalentWrapper> equivWrapper) {
            this.equiv = equivWrapper;
            return this;
        }

        public Builder withTimestamp(String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withDescription(List<Object> description) {
            this.description = description;
            return this;
        }

        public EquivalenceResultWrapper build() {
            return new EquivalenceResultWrapper(this);
        }
    }

}
