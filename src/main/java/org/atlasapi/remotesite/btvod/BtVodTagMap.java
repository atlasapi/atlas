package org.atlasapi.remotesite.btvod;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;

import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

/** BT VOD genre map for MetaBroadcast tags.
 *
 * This class is used in BtVodDescribedFieldsExtractor for mapping the BT VOD genres to
 * MetaBroadcast tags for the described object. This is done so that we can use BT VOD
 * content in the content prioritization algorithm. This further would allow us to filter
 * the BT VOD content by the priority.
 */
public class BtVodTagMap {

    private final ImmutableMap<String, String> btVodTagMap;
    private final String BT_VOD_NAMESPACE = "vod.bt.com";
    private final String METABROADCAST_TAG = "http://metabroadcast.com/tags/";
    private TopicStore topicStore;
    private MongoSequentialIdGenerator idGenerator;

    public BtVodTagMap(TopicStore topicStore, MongoSequentialIdGenerator idGenerator) {
        this.topicStore = checkNotNull(topicStore);
        this.idGenerator = checkNotNull(idGenerator);
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        // Film genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Film", "films");
        mapBuilder.put("http://vod.bt.com/genres/Short Film", "films");
        mapBuilder.put("http://vod.bt.com/genres/Action", "action");
        mapBuilder.put("http://vod.bt.com/genres/Suspense", "action");
        mapBuilder.put("http://vod.bt.com/genres/Family", "family");
        mapBuilder.put("http://vod.bt.com/genres/Horror", "horror");
        mapBuilder.put("http://vod.bt.com/genres/Zombie", "horror");
        mapBuilder.put("http://vod.bt.com/genres/Monsters", "horror");
        mapBuilder.put("http://vod.bt.com/genres/Gore", "horror");
        mapBuilder.put("http://vod.bt.com/genres/Indie", "indie");
        mapBuilder.put("http://vod.bt.com/genres/Romance", "romance");
        mapBuilder.put("http://vod.bt.com/genres/Supernatural", "scifi");
        mapBuilder.put("http://vod.bt.com/genres/Sci-Fi", "scifi");
        mapBuilder.put("http://vod.bt.com/genres/Comedy", "comedy");
        mapBuilder.put("http://vod.bt.com/genres/Drama", "drama");
        mapBuilder.put("http://vod.bt.com/genres/Melodrama", "drama");
        mapBuilder.put("http://vod.bt.com/genres/Thriller", "thriller");
        mapBuilder.put("http://vod.bt.com/genres/War", "war");

        // Factual genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Documentary", "factual");
        mapBuilder.put("http://vod.bt.com/genres/History", "history-biogs");
        mapBuilder.put("http://vod.bt.com/genres/Biopic", "history-biogs");
        mapBuilder.put("http://vod.bt.com/genres/Biography", "history-biogs");
        mapBuilder.put("http://vod.bt.com/genres/Educational", "learning");
        mapBuilder.put("http://vod.bt.com/genres/Nature", "nature-environment");
        mapBuilder.put("http://vod.bt.com/genres/People & Culture", "people-society");
        mapBuilder.put("http://vod.bt.com/genres/Cult", "religion-ethics");
        mapBuilder.put("http://vod.bt.com/genres/Science & Technology", "science-tech");

        // Lifestyle genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Lifestyle", "lifestyle");
        mapBuilder.put("http://vod.bt.com/genres/Food", "food-drink");
        mapBuilder.put("http://vod.bt.com/genres/Home", "home-garden");
        mapBuilder.put("http://vod.bt.com/genres/Garden", "home-garden");
        mapBuilder.put("http://vod.bt.com/genres/Fashion", "shopping-fashion");

        // Sports genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Sport", "sport");
        mapBuilder.put("http://vod.bt.com/genres/Cricket", "cricket");
        mapBuilder.put("http://vod.bt.com/genres/Football", "football");
        mapBuilder.put("http://vod.bt.com/genres/Archive Football", "football");
        mapBuilder.put("http://vod.bt.com/genres/Motorsport", "motorsport");
        mapBuilder.put("http://vod.bt.com/genres/Moto GP", "motorsport");
        mapBuilder.put("http://vod.bt.com/genres/Sports News", "news-roundups");
        mapBuilder.put("http://vod.bt.com/genres/Rugby", "rugby-league");
        mapBuilder.put("http://vod.bt.com/genres/Sports Documentary", "sports-events");
        mapBuilder.put("http://vod.bt.com/genres/Tennis", "tennis");
        mapBuilder.put("http://vod.bt.com/genres/Extreme Sports", "winter-extreme-sports");
        mapBuilder.put("http://vod.bt.com/genres/Winter Sports", "winter-extreme-sports");

        // Childrens genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Junior Girl and Boy", "childrens");
        mapBuilder.put("http://vod.bt.com/genres/Kids", "childrens");
        mapBuilder.put("http://vod.bt.com/genres/Children's", "childrens");
        mapBuilder.put("http://vod.bt.com/genres/Adventure", "action-adventure");
        mapBuilder.put("http://vod.bt.com/genres/Pre-school", "pre-school");
        mapBuilder.put("http://vod.bt.com/genres/Preschool", "pre-school");

        // Comedy genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Animation", "animated");
        mapBuilder.put("http://vod.bt.com/genres/Anime", "animated");
        mapBuilder.put("http://vod.bt.com/genres/Sitcoms", "sitcom-sketch");
        mapBuilder.put("http://vod.bt.com/genres/Stand-Up", "stand-up");
        mapBuilder.put("http://vod.bt.com/genres/Teen", "teen");
        mapBuilder.put("http://vod.bt.com/genres/Youth", "teen");

        // Drama genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Crime", "crime");
        mapBuilder.put("http://vod.bt.com/genres/Police", "crime");
        mapBuilder.put("http://vod.bt.com/genres/Detective", "crime");
        mapBuilder.put("http://vod.bt.com/genres/Period", "historical-period");
        mapBuilder.put("http://vod.bt.com/genres/Medical", "medical");

        // Entertainment genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Entertainment", "entertainment");
        mapBuilder.put("http://vod.bt.com/genres/Talk Show", "chat-shows");
        mapBuilder.put("http://vod.bt.com/genres/Celebrity", "celeb-reality");
        mapBuilder.put("http://vod.bt.com/genres/Reality", "celeb-reality");

        // Music genre mapping
        mapBuilder.put("http://vod.bt.com/genres/Music", "music");
        mapBuilder.put("http://vod.bt.com/genres/Karaoke", "music");

        // News genre mapping
        mapBuilder.put("http://vod.bt.com/genres/News", "news-weather");

        btVodTagMap = mapBuilder.build();
    }

    /** This method maps BT VOD content genres with MetaBroadcast tags.
     * This is done to get MetaBroadcast tags for a specific BT VOD content. After that the
     * BT VOD content tags are added to the BT VOD content topicRef field.
     * @param genres - BT VOD content genres that are used for mapping with MetaBroadcast tags.
     * @return set of MetaBroadcast tags as TopicRef objects for the BT VOD content.
     */
    public Set<TopicRef> map(Set<String> genres) {
        Set<String> tags = Sets.newHashSet();
        for (String genre : genres) {
            if (genre.contains("http://vod.bt.com/genres/")) {
                String tag = btVodTagMap.get(genre);
                if (tag != null) {
                    tags.add(tag);
                }
            }
        }

        return getTopicRefFromTags(tags);
    }

    /** This method is used for creating a set of TopicRefs from mapped tags.
     * This is done because we store MetaBroadcast tags as TopicRefs to the BT VOD content.
     * @param mappedTags - mapped tags for the BT VOD content.
     * @return set of MetaBroadcast tags as TopicRef objects for the BT VOD content.
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

            topic.setPublisher(Publisher.BT_VOD);
            topic.setTitle(tag);
            topic.setType(Topic.Type.UNKNOWN);
            topicStore.write(topic);

            topicRefBuilder.add(new TopicRef(
                    topic, 0f, false, TopicRef.Relationship.ABOUT, 0)
            );
        } else {
            Topic topic = new Topic(
                    idGenerator.generateRaw(),
                    BT_VOD_NAMESPACE,
                    METABROADCAST_TAG + tag);
            topic.setPublisher(Publisher.BT_VOD);
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
        return topicStore.topicFor(Publisher.BT_VOD, BT_VOD_NAMESPACE, METABROADCAST_TAG + tag);
    }
}