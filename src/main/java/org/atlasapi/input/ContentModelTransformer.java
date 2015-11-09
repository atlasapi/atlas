package org.atlasapi.input;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.Topic.Type;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.TopicRef.Relationship;
import org.atlasapi.media.entity.simple.*;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

import javax.annotation.Nullable;

public abstract class ContentModelTransformer<F extends Description,T extends Content> extends DescribedModelTransformer<F, T> {
    private final TopicStore topicStore;
    private final LookupEntryStore lookupStore;
    private final NumberToShortStringCodec idCodec;
    protected final Clock clock;
    private final ClipModelTransformer clipsModelTransformer;
    
    private final Predicate<SameAs> SAMEAS_WITH_IDS_FILTER = new Predicate<SameAs>() {

        @Override
        public boolean apply(SameAs input) {
            return !Strings.isNullOrEmpty(input.getId());
        }
        
    };
    
    private final Predicate<SameAs> SAMEAS_WITHOUT_IDS_FILTER = new Predicate<SameAs>() {

        @Override
        public boolean apply(SameAs input) {
            return Strings.isNullOrEmpty(input.getId());
        }
        
    };
    
    private final Function<SameAs, Long> SAMEAS_TO_ID_TRANSFORMER = new Function<SameAs, Long>() {

        @Override
        public Long apply(SameAs input) {
            return idCodec.decode(input.getId()).longValue();
        }
        
    };
    
    private final Function<SameAs, String> SAMEAS_TO_URI_TRANSFORMER = new Function<SameAs, String>() {
        @Override
        public String apply(SameAs input) {
            return input.getUri();
        }
    };

    public ContentModelTransformer(LookupEntryStore lookupStore, TopicStore topicStore, 
            NumberToShortStringCodec idCodec, ClipModelTransformer clipsModelTransformer, Clock clock) {
        super(clock);
        this.lookupStore = lookupStore;
        this.topicStore = topicStore;
        this.clipsModelTransformer = clipsModelTransformer;
        this.idCodec = idCodec;
        this.clock = clock;
    }

    @Override
    protected T setFields(T result, F inputContent) {
        super.setFields(result, inputContent);
        result.setPeople(transformPeople(inputContent.getPeople(), result.getPublisher()));
        result.setTopicRefs(topicRefs(inputContent.getTopics()));
        result.setKeyPhrases(keyPhrases(inputContent.getKeyPhrases(), inputContent.getPublisher()));
        result.setGenres(inputContent.getGenres());
        result.setClips(transformClips(inputContent));
        result.setEventRefs(eventRefs(inputContent.getEventRefs()));
        return result;
    }

    private Iterable<Clip> transformClips(Description inputContent) {
        if (inputContent.getClips() == null) {
            return ImmutableList.<Clip>of();
        }
        List<Clip> clips = Lists.newArrayListWithCapacity(inputContent.getClips().size());
        for (org.atlasapi.media.entity.simple.Item inputClip : inputContent.getClips()) {
            Clip clip = (Clip) this.clipsModelTransformer.transform(inputClip);
            clip.setClipOf(inputContent.getUri());
            clips.add(clip);
        }
        return clips;
    }
    
    private Iterable<KeyPhrase> keyPhrases(Iterable<org.atlasapi.media.entity.simple.KeyPhrase> keyPhrases, final PublisherDetails contentPublisher) {
        return ImmutableList.copyOf(Iterables.transform(keyPhrases, new Function<org.atlasapi.media.entity.simple.KeyPhrase, KeyPhrase>() {

            @Override
            public KeyPhrase apply(org.atlasapi.media.entity.simple.KeyPhrase input) {
                Preconditions.checkState(input.getPublisher() == null || input.getPublisher().getKey().equals(contentPublisher.getKey()), 
                        "Publisher in key phrase must match publisher for content");
                PublisherDetails publisherDetails = Objects.firstNonNull(input.getPublisher(), contentPublisher);
                Maybe<Publisher> publisher = Publisher.fromKey(publisherDetails.getKey());

                if(!publisher.hasValue()) {
                    throw new IllegalArgumentException(String.format("No publisher for %s", publisherDetails.getKey()));
                }

                return new KeyPhrase(input.getPhrase(), publisher.requireValue(), input.getWeighting());
            }
        }));
    }

    private Iterable<TopicRef> topicRefs(Set<org.atlasapi.media.entity.simple.TopicRef> topics) {
        return ImmutableSet.copyOf(Iterables.transform(topics, new Function<org.atlasapi.media.entity.simple.TopicRef, TopicRef>() {

            @Override
            public TopicRef apply(org.atlasapi.media.entity.simple.TopicRef input) {
                org.atlasapi.media.entity.simple.Topic inputTopic = input.getTopic();
                String value = inputTopic.getValue();
                String namespace = inputTopic.getNamespace();
                Publisher publisher = getPublisher(inputTopic.getPublisher());
                if (isNullOrEmpty(value) || isNullOrEmpty(namespace)) {
                    throw new IllegalArgumentException("Topic missing value or namespace");
                }
                Maybe<Topic> possibleTopic = topicStore.topicFor(publisher, namespace, value);
                if (possibleTopic.hasValue()) {
                    Topic topic = possibleTopic.requireValue();
                    updateTopic(inputTopic, topic);
                    topicStore.write(topic);
                    return new TopicRef(
                        topic, 
                        input.getWeighting(),
                        input.isSupervised(), 
                        Relationship.fromString(input.getRelationship()).orNull(),
                        input.getOffset()
                    );
                } else {
                    throw new IllegalStateException(
                        String.format("No topic for %s/%s/%s",publisher,namespace,value)
                    );
                }
            }

            private void updateTopic(org.atlasapi.media.entity.simple.Topic inputTopic, Topic topic) {
                if (inputTopic.getType() != null && topic.getType() == null) {
                    topic.setType(Type.fromKey(inputTopic.getType()));
                }
                if (topic.getTitle() == null) {
                    topic.setTitle(inputTopic.getTitle());
                }
                if (topic.getDescription() == null) {
                    topic.setDescription(inputTopic.getDescription());
                }
                if (topic.getImage() == null) {
                    topic.setImage(inputTopic.getImage());
                }
                if (topic.getThumbnail() == null) {
                    topic.setThumbnail(inputTopic.getThumbnail());
                }
            }
            
        }));
    }

    private Iterable<EventRef> eventRefs(Set<org.atlasapi.media.entity.simple.EventRef> eventRef){
        return ImmutableSet.copyOf(Iterables.transform(eventRef, new Function<org.atlasapi.media.entity.simple.EventRef, EventRef>() {
            @Override
            public EventRef apply(org.atlasapi.media.entity.simple.EventRef input) {
                return new EventRef(idCodec.decode(input.getId()).longValue(), getPublisher(input.getPublisher()));
            }
        }));
    }

    @Override
    protected final Set<LookupRef> resolveEquivalents(Set<String> sameAs) {
        return resolveEquivs(sameAs);
    }
    
    @Override
    protected Set<LookupRef> resolveSameAs(Set<SameAs> equivalents) {
        ImmutableSet.Builder<LookupRef> lookups = new ImmutableSet.Builder<LookupRef>();
        Iterable<Long> idsToResolve = Iterables.transform( 
            Iterables.filter(equivalents, SAMEAS_WITH_IDS_FILTER),
            SAMEAS_TO_ID_TRANSFORMER);
        Iterable<String> urisToResolve = Iterables.transform(
            Iterables.filter(equivalents, SAMEAS_WITHOUT_IDS_FILTER), 
            SAMEAS_TO_URI_TRANSFORMER);
        
        lookups.addAll(Iterables.transform(
                 lookupStore.entriesForIds(idsToResolve), 
                                LookupEntry.TO_SELF));
        lookups.addAll(resolveEquivs(urisToResolve));
        return lookups.build();
    }

    private Set<LookupRef> resolveEquivs(Iterable<String> sameAs) {
        return ImmutableSet.copyOf( Iterables.transform(
             lookupStore.entriesForCanonicalUris(sameAs),
             LookupEntry.TO_SELF));
    }

    private List<CrewMember> transformPeople(List<Person> people, Publisher publisher) {
        List<CrewMember> crew = Lists.newArrayListWithCapacity(people.size());
        for (Person person : people) {
            crew.add(transformPerson(person, publisher));
        }
        return crew;
    }

    private CrewMember transformPerson(Person person, Publisher publisher) {
        CrewMember member;
        checkNotNull(person.getUri(), "person requires uri");
        if ("actor".equals(person.getType())) {
            member = new Actor().withCharacter(person.getCharacter());
        } else {
            member = new CrewMember().withRole(Role.fromPossibleKey(person.getRole()).valueOrNull());
        }
        member.withPublisher(publisher);
        member.withName(person.getName());
        // TODO new alias
        member.setAliasUrls(person.getAliases());
        member.setCanonicalUri(person.getUri());
        member.setCurie(person.getCurie());
        return member;
    }
}
