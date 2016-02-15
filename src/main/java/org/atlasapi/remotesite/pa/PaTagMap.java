package org.atlasapi.remotesite.pa;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.atlasapi.media.entity.Publisher.PA;

/** PA genre map for MetaBroadcast tags.
 *
 * This class is used in PaProgrammeProcessor for mapping the PA genres to
 * MetaBroadcast tags for the brand, episode and series content. Tags are stored
 * as a topics as part of the PA content. This is done so that we can use PA content
 * in the content prioritization algorithm. This further would allow us to filter
 * the PA content by the priority.
 */
public class PaTagMap {

    private final ImmutableMap<String, String> paTagMap;
    private final String PA_NAMESPACE = "pressassociation";
    private final String METABROADCAST_TAG = "http://metabroadcast.com/tags/";
    private TopicStore topicStore;
    private DatabasedMongo mongo;

    public PaTagMap(TopicStore topicStore, DatabasedMongo mongo) {
        this.topicStore = topicStore;
        this.mongo = mongo;
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        // Films genre mapping
        mapBuilder.put("http://pressassociation.com/genres/BF01", "films");
        mapBuilder.put("http://pressassociation.com/genres/1F18", "bollywood-world");
        mapBuilder.put("http://pressassociation.com/genres/1F06", "family");
        mapBuilder.put("http://pressassociation.com/genres/1F03", "horror");
        mapBuilder.put("http://pressassociation.com/genres/1F1A", "indie");
        mapBuilder.put("http://pressassociation.com/genres/1600", "romance");
        mapBuilder.put("http://pressassociation.com/genres/1300", "scifi");
        mapBuilder.put("http://pressassociation.com/genres/1000", "drama");
        mapBuilder.put("http://pressassociation.com/genres/1F19", "factual");
        mapBuilder.put("http://pressassociation.com/genres/1F0A", "thriller");
        mapBuilder.put("http://pressassociation.com/genres/1F10", "war");

        // Factual genre mapping
        mapBuilder.put("http://pressassociation.com/genres/9000", "factual");
        mapBuilder.put("http://pressassociation.com/genres/7000", "arts");
        mapBuilder.put("http://pressassociation.com/genres/2200", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/2300", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/2F07", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/2F08", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/8000", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/8200", "current-affairs-politics");
        mapBuilder.put("http://pressassociation.com/genres/9F05", "history-biogs");
        mapBuilder.put("http://pressassociation.com/genres/9F00", "learning");
        mapBuilder.put("http://pressassociation.com/genres/9100", "nature-environment");
        mapBuilder.put("http://pressassociation.com/genres/9F07", "nature-environment");
        mapBuilder.put("http://pressassociation.com/genres/9F08", "nature-environment");
        mapBuilder.put("http://pressassociation.com/genres/9F09", "nature-environment");
        mapBuilder.put("http://pressassociation.com/genres/9F0A", "nature-environment");
        mapBuilder.put("http://pressassociation.com/genres/7400", "people-society");
        mapBuilder.put("http://pressassociation.com/genres/9500", "religion-ethics");
        mapBuilder.put("http://pressassociation.com/genres/9200", "science-tech");
        mapBuilder.put("http://pressassociation.com/genres/9400", "travel");

        // Lifestyle genre mapping
        mapBuilder.put("http://pressassociation.com/genres/A000", "lifestyle");
        mapBuilder.put("http://pressassociation.com/genres/9F0B", "family-friends");
        mapBuilder.put("http://pressassociation.com/genres/A500", "food-drink");
        mapBuilder.put("http://pressassociation.com/genres/A400", "health-beauty");
        mapBuilder.put("http://pressassociation.com/genres/A700", "home-garden");
        mapBuilder.put("http://pressassociation.com/genres/AF00", "home-garden");
        mapBuilder.put("http://pressassociation.com/genres/AF03", "home-garden");
        mapBuilder.put("http://pressassociation.com/genres/AF01", "home-garden");
        mapBuilder.put("http://pressassociation.com/genres/A200", "home-garden");
        mapBuilder.put("http://pressassociation.com/genres/A100", "leisure-holidays");
        mapBuilder.put("http://pressassociation.com/genres/A300", "motors-gadgets");
        mapBuilder.put("http://pressassociation.com/genres/AF02", "motors-gadgets");
        mapBuilder.put("http://pressassociation.com/genres/A600", "shopping-fashion");
        mapBuilder.put("http://pressassociation.com/genres/7B00", "shopping-fashion");

        // Sport genre mapping
        mapBuilder.put("http://pressassociation.com/genres/4000", "sport");
        mapBuilder.put("http://pressassociation.com/genres/4F0C", "cricket");
        mapBuilder.put("http://pressassociation.com/genres/4F0D", "cricket");
        mapBuilder.put("http://pressassociation.com/genres/4300", "football");
        mapBuilder.put("http://pressassociation.com/genres/4F15", "football");
        mapBuilder.put("http://pressassociation.com/genres/4F16", "football");
        mapBuilder.put("http://pressassociation.com/genres/4F19", "golf");
        mapBuilder.put("http://pressassociation.com/genres/4F1D", "horse-racing");
        mapBuilder.put("http://pressassociation.com/genres/4700", "motorsport");
        mapBuilder.put("http://pressassociation.com/genres/4F17", "motorsport");
        mapBuilder.put("http://pressassociation.com/genres/4F21", "motorsport");
        mapBuilder.put("http://pressassociation.com/genres/4F2B", "motorsport");
        mapBuilder.put("http://pressassociation.com/genres/4200", "news-roundups");
        mapBuilder.put("http://pressassociation.com/genres/4F2D", "rugby-league");
        mapBuilder.put("http://pressassociation.com/genres/4F2E", "rugby-league");
        mapBuilder.put("http://pressassociation.com/genres/4F2F", "rugby-union");
        mapBuilder.put("http://pressassociation.com/genres/4F30", "rugby-union");
        mapBuilder.put("http://pressassociation.com/genres/4100", "sports-events");
        mapBuilder.put("http://pressassociation.com/genres/4F24", "sports-events");
        mapBuilder.put("http://pressassociation.com/genres/4400", "tennis");
        mapBuilder.put("http://pressassociation.com/genres/4900", "winter-extreme-sports");
        mapBuilder.put("http://pressassociation.com/genres/4F11", "winter-extreme-sports");

        // Childrens genre mapping
        mapBuilder.put("http://pressassociation.com/genres/5000", "childrens");
        mapBuilder.put("http://pressassociation.com/genres/5500", "cartoons");
        mapBuilder.put("http://pressassociation.com/genres/5F00", "drama");
        mapBuilder.put("http://pressassociation.com/genres/5F01", "funnies");
        mapBuilder.put("http://pressassociation.com/genres/5F02", "games-quizzes");
        mapBuilder.put("http://pressassociation.com/genres/5400", "kids-learning");
        mapBuilder.put("http://pressassociation.com/genres/5100", "pre-school");

        // Comedy genre mapping
        mapBuilder.put("http://pressassociation.com/genres/1400", "comedy");
        mapBuilder.put("http://pressassociation.com/genres/1F01", "animated");
        mapBuilder.put("http://pressassociation.com/genres/1F12", "sitcom-sketch");
        mapBuilder.put("http://pressassociation.com/genres/3F05", "stand-up");

        // Drama genre mapping
        mapBuilder.put("http://pressassociation.com/genres/1100", "crime");
        mapBuilder.put("http://pressassociation.com/genres/1F17", "historical-period");
        mapBuilder.put("http://pressassociation.com/genres/1F07", "medical");
        mapBuilder.put("http://pressassociation.com/genres/1500", "soap");

        // Entertainment genre mapping
        mapBuilder.put("http://pressassociation.com/genres/3000", "entertainment");
        mapBuilder.put("http://pressassociation.com/genres/3300", "chat-shows");
        mapBuilder.put("http://pressassociation.com/genres/3100", "games-quizzes");
        mapBuilder.put("http://pressassociation.com/genres/3200", "talent-variety");
        mapBuilder.put("http://pressassociation.com/genres/3F03", "celeb-reality");

        // Music genre mapping
        mapBuilder.put("http://pressassociation.com/genres/6000", "music");

        // News genre mapping
        mapBuilder.put("http://pressassociation.com/genres/2F02", "news-weather");
        mapBuilder.put("http://pressassociation.com/genres/2F03", "news-weather");
        mapBuilder.put("http://pressassociation.com/genres/2F04", "news-weather");
        mapBuilder.put("http://pressassociation.com/genres/2F05", "news-weather");
        mapBuilder.put("http://pressassociation.com/genres/2F06", "news-weather");

        paTagMap = mapBuilder.build();
    }

    /** This method maps PA content genres with MetaBroadcast tags.
     *
     * This is done to get MetaBroadcast tags for a specific PA content. After that the
     * PA content tags are added to the PA content topicRef field.
     * @param genres - PA content genres that are used for mapping with MetaBroadcast tags.
     * @return set of MetaBroadcast tags as TopicRef objects for the PA content.
     */
    public Set<TopicRef> map(Set<String> genres) {
        Set<String> tags = Sets.newHashSet();
        for (String genre : genres) {
            if (genre.contains("http://pressassociation.com/genres/")) {
                String tag = paTagMap.get(genre);
                if (tag != null) {
                    tags.add(tag);
                }
            }
        }
        // Checking if the tags set has only a one tag with the value - film, this means that it's action film.
        if (tags.size() == 1 && tags.contains("film")) {
            tags.add("action");
        }
        return getTopicRefFromTags(tags);
    }

    /** This method is used for creating a set of TopicRefs from mapped tags.
     *
     * This is done because we store MetaBroadcast tags as TopicRefs to the PA content.
     * @param mappedTags - mapped tags for the PA content.
     * @return set of MetaBroadcast tags as TopicRef objects for the PA content.
     */
    private Set<TopicRef> getTopicRefFromTags(Set<String> mappedTags) {
        ImmutableSet<String> tags = ImmutableSet.copyOf(mappedTags);
        if(tags.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<TopicRef> topicRefBuilder = ImmutableSet.builder();
        for (String tag : tags) {
            addTopicRef(topicRefBuilder, tag);
        }

        return topicRefBuilder.build();
    }

    /** Used for adding TopicRef objects to the topicRefBuilder
     *
     * This method either creates TopicRef from existing Topic in the database,
     * or generates a new topic id to create a new Topic in a case where
     * Topic doesn't exist in the database.
     * @param topicRefBuilder ImmutableSet that holds all mapped tags TopicRefs
     * @param tag used for creating a TopicRef object.
     */
    public void addTopicRef(ImmutableSet.Builder<TopicRef> topicRefBuilder, String tag) {
        Maybe<Topic> resolvedTopic = resolveTopic(tag);
        if (resolvedTopic.hasValue()) {
            Topic topic = resolvedTopic.requireValue();

            topic.setPublisher(PA);
            topic.setTitle(tag);
            topic.setType(Topic.Type.UNKNOWN);
            topicStore.write(topic);

            topicRefBuilder.add(new TopicRef(
                    topic, 0f, false, TopicRef.Relationship.ABOUT, 0)
            );
        } else {
            IdGenerator idGenerator = new MongoSequentialIdGenerator(mongo, "topics");
            Topic topic = new Topic(
                    idGenerator.generateRaw(),
                    PA_NAMESPACE,
                    METABROADCAST_TAG + tag);
            topic.setPublisher(PA);
            topic.setTitle(tag);
            topic.setType(Topic.Type.UNKNOWN);
            topicStore.write(topic);

            topicRefBuilder.add(new TopicRef(
                    topic, 0f, false, TopicRef.Relationship.ABOUT, 0)
            );
        }
    }

    /** Resolves given tag to a possible Topic object
     *
     * This method is used to resolve Topic from a tag by making a call to
     * the topic Store. If topic exists would return topic wrapped in Maybe.
     * If there is no topic with given tag, would return empty value wrapped
     * in Maybe.
     * @param tag is used for looking up Topic in db
     * @return Maybe<Topic> resolved tag as a Topic from db
     */
    public Maybe<Topic> resolveTopic(String tag) {
        return topicStore.topicFor(Publisher.PA, PA_NAMESPACE, METABROADCAST_TAG + tag);
    }
}