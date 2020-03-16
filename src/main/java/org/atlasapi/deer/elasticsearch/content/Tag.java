package org.atlasapi.deer.elasticsearch.content;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;

// I have simplified this class to avoid importing other classes we don't need
public class Tag implements Hashable {

    private Id topic;
    private Publisher publisher;
    private Boolean supervised;
    private Float weighting;
    private Relationship relationship;
    private Integer offset;

    public static Tag valueOf(Id topicId, Float weighting, Boolean supervised,
            Relationship relationship) {
        return new Tag(topicId, weighting, supervised, relationship);
    }

    public Tag(long topicId, Float weighting, Boolean supervised, Relationship relationship) {
        this(Id.valueOf(topicId), weighting, supervised, relationship, null);
    }

    public Tag(Id topicId, Float weighting, Boolean supervised, Relationship relationship) {
        this(topicId, weighting, supervised, relationship, null);
    }

    public Tag(Id topicId, Float weighting, Boolean supervised, Relationship relationship,
            Integer offset) {
        this.topic = topicId;
        this.weighting = weighting;
        this.supervised = supervised;
        this.relationship = relationship;
        this.offset = offset;
    }

    public void setTopicUri(Id topic) {
        this.topic = topic;
    }

    public void setWeighting(Float weighting) {
        this.weighting = weighting;
    }

    public void setSupervised(Boolean supervised) {
        this.supervised = supervised;
    }

    public void setRelationship(Relationship relationship) {
        this.relationship = relationship;
    }

    @FieldName("weighting")
    public Float getWeighting() {
        return weighting;
    }

    @FieldName("is_supervised")
    public Boolean isSupervised() {
        return supervised;
    }

    @FieldName("topic")
    public Id getTopic() {
        return topic;
    }

    @FieldName("relationship")
    public Relationship getRelationship() {
        return relationship;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    @FieldName("offset")
    public Integer getOffset() {
        return this.offset;
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    @FieldName("publisher")
    public Publisher getPublisher() {
        return publisher;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(supervised, topic, weighting, relationship);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Tag) {
            Tag other = (Tag) that;
            return Objects.equal(supervised, other.supervised)
                    && Objects.equal(weighting, other.weighting)
                    && Objects.equal(topic, other.topic)
                    && Objects.equal(relationship, other.relationship);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(
                "Ref topic %s, %+.2f, %s supervised, with relationship %s",
                topic,
                weighting,
                supervised ? "" : "not",
                relationship
        );
    }

    public enum Relationship {

        ABOUT("about"),
        TWITTER_AUDIENCE("twitter:audience"),
        TWITTER_AUDIENCE_RELATED("twitter:audience-related"),
        TWITTER_AUDIENCE_REALTIME("twitter:audience:realtime"),
        TRANSCRIPTION("transcription"),
        TRANSCRIPTION_SUBTITLES("transcription:subtitles"),
        TRANSCRIPTION_SUBTITLES_REALTIME("transcription:subtitles:realtime");
        private final String name;

        Relationship(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        private static ImmutableSet<Relationship> ALL = ImmutableSet.copyOf(values());

        public static ImmutableSet<Relationship> all() {
            return ALL;
        }

        private static ImmutableMap<String, Optional<Relationship>> LOOKUP = ImmutableMap.copyOf(
                Maps.transformValues(
                        Maps.uniqueIndex(all(), Functions.toStringFunction()),
                        new Function<Relationship, Optional<Relationship>>() {

                            @Override
                            public Optional<Relationship> apply(@Nullable Relationship input) {
                                return Optional.fromNullable(input);
                            }
                        }
                ));

        public static Optional<Relationship> fromString(String relationship) {
            Optional<Relationship> possibleRelationship = LOOKUP.get(relationship);
            return possibleRelationship != null ? possibleRelationship
                                                : Optional.absent();
        }
    }
}
