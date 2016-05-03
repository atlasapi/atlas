package org.atlasapi.remotesite.events;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.media.entity.Publisher.DBPEDIA;


public abstract class EventsUtility<S> {

    private static final String DBPEDIA_NAMESPACE = "dbpedia";
    
    private final Function<EventGroup, Topic> topicLookup = new Function<EventGroup, Topic>() {
        @Override
        public Topic apply(EventGroup input) {
            return resolveOrCreateDbpediaTopic(
                    input.getTitle(),
                    Topic.Type.SUBJECT,
                    input.getNamespace(),
                    input.getValue(),
                    input.getPublisher()
            );
        }
    };
    private final TopicStore topicStore;
    
    public EventsUtility(TopicStore topicStore)  {
        this.topicStore = checkNotNull(topicStore);
    }
    
    public abstract String createEventUri(String id);
    
    public abstract String createTeamUri(OptaSportType sportType, String id);
    
    /**
     * Where an end time has not been provided for an {@link org.atlasapi.media.entity.Event},
     * one can be estimated based on which sport is being played. 
     * @param start
     * @return An approximate end time for the event, or Optional.absent if the sport does 
     * not have a mapped duration
     */
    public abstract Optional<DateTime> createEndTime(S sport, DateTime start);
    
    /**
     * For a given location String, looks up a matching DBpedia Topic and returns 
     * it.
     * @param location
     * @return An Optional containing the Topic, or Optional.absent if no topic 
     * found for the provided location
     */
    public Optional<Topic> createOrResolveVenue(String location) {
        Optional<LocationTitleUri> value = fetchLocationUrl(location);
        if (!value.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(resolveOrCreateDbpediaTopic(
                value.get().title, Topic.Type.PLACE, DBPEDIA_NAMESPACE, value.get().uri, DBPEDIA
        ));
    }

    public static class LocationTitleUri {
        final public String title;
        final public String uri;

        public LocationTitleUri(String title, String uri) {
            this.title = checkNotNull(title);
            this.uri = checkNotNull(uri);
        }
    }

    public abstract Optional<LocationTitleUri> fetchLocationUrl(String location);
    
    /**
     * For a given sport, looks up a set of DBpedia Topic value Strings associated
     * with that sport, parses them to Topics and returns.
     * <p> 
     * For example, Rugby might be associated with the topics for Rugby League and 
     * Rugby Football.
     * @param sport
     * @return Optional containing Set of dbpedia Topics, or Optional.absent if no
     * values found for the provided sport
     */
    public Optional<Set<Topic>> parseEventGroups(S sport) {
        Optional<List<EventGroup>> eventGroups = fetchEventGroupUrls(sport);
        if (!eventGroups.isPresent()) {
            return Optional.absent();
        }
        return Optional.<Set<Topic>>of(ImmutableSet.copyOf(Iterables.transform(
                eventGroups.get(), topicLookup
        )));
    }
    
    public abstract Optional<List<EventGroup>> fetchEventGroupUrls(S sport);
    
    private Topic resolveOrCreateDbpediaTopic(String title, Topic.Type topicType, String namespace,
            String value, Publisher publisher) {
        Maybe<Topic> resolved = topicStore.topicFor(namespace, value);
        if (resolved.hasValue()) {
            Topic topic = resolved.requireValue();
            
            topic.setPublisher(publisher);
            topic.setTitle(title);
            topic.setType(topicType);
            
            topicStore.write(topic);
            
            return topic;
        }
        throw new IllegalStateException(String.format(
                "Topic store failed to create Topic with namespace %s and value %s", 
                namespace,
                value
        ));
    }

    public static class EventGroup {

        private final String title;
        private final String namespace;
        private final String value;
        private final Publisher publisher;

        private EventGroup(String title, String namespace, String value, Publisher publisher) {
            this.title = title;
            this.namespace = namespace;
            this.value = value;
            this.publisher = publisher;
        }

        public static EventGroup of(String title, String namespace, String value,
                Publisher publisher) {
            return new EventGroup(title, namespace, value, publisher);
        }

        public static EventGroup ofDefaultNs(String title, String value) {
            return new EventGroup(title, DBPEDIA_NAMESPACE, value, DBPEDIA);
        }

        public String getTitle() {
            return title;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getValue() {
            return value;
        }

        public Publisher getPublisher() {
            return publisher;
        }
    }
}
