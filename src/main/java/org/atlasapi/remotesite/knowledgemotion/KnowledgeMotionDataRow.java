package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class KnowledgeMotionDataRow {

    private final String source;
    private final String id;
    private final String title;
    private final String description;
    private final String date;
    private final String duration;
    private final List<String> keywords;
    private final String alternativeId;

    public static Builder builder() {
        return new Builder();
    }

    public KnowledgeMotionDataRow(String source, String id, String title, String description,
            String date, String duration, Iterable<String> keywords, String alternativeId) {
        this.source = checkNotNull(source);
        this.id = checkNotNull(id);
        this.title = checkNotNull(title);
        this.description = checkNotNull(description);
        this.date = checkNotNull(date);
        this.duration = checkNotNull(duration);
        this.keywords = ImmutableList.copyOf(keywords);
    }
    
    public String getSource() {
        return source;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getDate() {
        return date;
    }

    public String getDuration() {
        return duration;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(super.toString())
                .add("source", source)
                .add("id", id)
                .add("title", title)
                .add("description", description)
                .add("date", date)
                .add("duration", duration)
                .add("keywords", keywords)
                .toString();
    }

    public static class Builder {

        private String source;
        private String id;
        private String title;
        private String description;
        private String date;
        private String duration;
        private List<String> keywords = ImmutableList.of();
        private String alternativeId;

        public KnowledgeMotionDataRow build() {
            return new KnowledgeMotionDataRow(source, id, title, description, date, duration, keywords, alternativeId);
        }

        private Builder() {}

        public Builder withSource(String source) {
            this.source = source;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withDate(String date) {
            this.date = date;
            return this;
        }
        public Builder withDuration(String duration) {
            this.duration = duration;
            return this;
        }

        public Builder withKeywords(Iterable<String> keywords) {
            this.keywords = ImmutableList.copyOf(keywords);
            return this;
        }

        public Builder withAlternativeId(String altId) {
            this.alternativeId = altId;
            return this;
        }

    }

}
