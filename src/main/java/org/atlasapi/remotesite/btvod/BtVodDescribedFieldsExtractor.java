package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Maps;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Image;

import com.google.common.collect.Iterables;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BtVodDescribedFieldsExtractor {

    static final String GUID_ALIAS_NAMESPACE = "gb:bt:tv:mpx:prod:guid";
    static final String ID_ALIAS_NAMESPACE = "gb:bt:tv:mpx:prod:id";
    static final String CONTENT_PROVIDER_TOPIC_NAMESPACE = "gb:bt:tv:mpx:prod:contentProvider";
    private static final String BT_VOD_GENRE_PREFIX = "http://vod.bt.com/genres";

    private static final String YOUVIEW_GENRE_PREFIX = "http://youview.com/genres";
    private static final Pattern SUB_GENRE_ARRAYPATTERN = Pattern.compile("^\\[(.*)\\]$");
    private static final CSVFormat SUB_GENRE_CSV_PARSER = CSVFormat.RFC4180;
    private final ImageExtractor imageExtractor;

    private final TopicCreatingTopicResolver topicCreatingTopicResolver;
    private final TopicWriter topicWriter;
    private final Map<String, Topic> topics = Maps.newConcurrentMap();

    private static final Map<String, String> BT_TO_YOUVIEW_GENRE = ImmutableMap.<String,String>builder()
    .put("Talk Show", ":FormatCS:2010:2.1.5")
    .put("Classical", ":ContentCS:2010:3.6.1")
    .put("Soundtracks", ":ContentCS:2010:3.6.3.8")
    .put("Country", ":ContentCS:2010:3.6.6")
    .put("Folk", ":ContentCS:2010:3.6.9")
    .put("Soul and Motown", ":ContentCS:2010:3.6.5.1")
    .put("Rock and Roll", ":ContentCS:2010:3.6.4.14")
    .put("Oldies", ":ContentCS:2010:3.6.3.5")
    .put("Jazz", ":ContentCS:2010:3.6.2")
    .put("Hip Hop and Rap", ":ContentCS:2010:3.6.7.2")
    .put("Alternative Rock", ":ContentCS:2010:3.6.4.14.6")
    .put("Classic_Rock", ":ContentCS:2010:3.6.4.14")
    .put("Bollywood", ":ContentCS:2010:3.6.17.3")
    .put("Wrestling", ":ContentCS:2010:3.2.13.6")
    .put("World of Sport", ":ContentCS:2010:3.2")
    .put("Winter Sports", ":ContentCS:2010:3.2.7")
    .put("Westerns", ":ContentCS:2010:3.4.6.9")
    .put("Volleyball", ":ContentCS:2010:3.2.3.21")
    .put("UFC", ":ContentCS:2010:3.2.12.2")
    .put("Thriller", ":ContentCS:2010:3.4.6.10")
    .put("Tennis", ":ContentCS:2010:3.2.4.7")
    .put("Ten Pin Bowling", ":ContentCS:2010:3.2.9.3")
    .put("Swimming and Diving", ":ContentCS:2010:3.2.6.4")
    .put("Swimming", ":ContentCS:2010:3.2.6.11")
    .put("Sports News", ":ContentCS:2010:3.1.1.9")
    .put("Sports Documentary", ":FormatCS:2010:2.1.4")
    .put("Soaps", ":ContentCS:2010:3.4.2")
    .put("Snooker", ":ContentCS:2010:3.2.9.8")
    .put("Sci-Fi and Fantasy", ":ContentCS:2010:3.4.7")
    .put("Sci-Fi", ":ContentCS:2010:3.4.6.7")
    .put("Rock", ":ContentCS:2010:3.6.4.14")
    .put("Reggae", ":ContentCS:2010:3.6.7.3")
    .put("Reality", ":ContentCS:2010:3.5.5")
    .put("R and B", ":ContentCS:2010:3.6.5.2")
    .put("Quiz Shows", ":ContentCS:2010:3.5.2")
    .put("Pre-school", ":ContentCS:2010:3.1.3.6.1")
    .put("Pool", ":ContentCS:2010:3.2.9.8")
    .put("Poker", ":ContentCS:2010:3.2.22.3")
    .put("Paralympics", "/YouViewEventCS/2012-02-06#10.1.7")
    .put("Musicals", ":ContentCS:2010:3.4.10")
    .put("Motorsport", ":ContentCS:2010:3.2.8")
    .put("Modern Rock", ":ContentCS:2010:3.6.4.14")
    .put("Love", ":ContentCS:2010:3.5.7.2")
    .put("pop", ":ContentCS:2010:3.6.4.1")
    .put("Indie", ":ContentCS:2010:3.6.4.14.6")
    .put("Ice Hockey", ":ContentCS:2010:3.2.7.3")
    .put("Horse Racing", ":ContentCS:2010:3.2.11.3")
    .put("Horror", ":ContentCS:2010:3.4.6.6")
    .put("Hockey", ":ContentCS:2010:3.2.3.14")
    .put("Gymnastics", ":ContentCS:2010:3.2.10")
    .put("Golf", ":ContentCS:2010:3.2.15")
    .put("Games Show", ":FormatCS:2010:2.4")
    .put("Football", ":ContentCS:2010:3.2.3.12")
    .put("Fishing", ":ContentCS:2010:3.2.6.5")
    .put("Family", ":ContentCS:2010:3.8.7")
    .put("Extreme Sports", ":ContentCS:2010:3.2.12.2")
    .put("Equestrian", ":ContentCS:2010:3.2.11")
    .put("Entertainment", ":ContentCS:2010:3.5")
    .put("Educational", ":IntentionCS:2005:1.3")
    .put("Diving", ":ContentCS:2010:3.2.6.4")
    .put("Darts", ":ContentCS:2010:3.2.9.6")
    .put("Dance", ":ContentCS:2010:3.6.8")
    .put("Cycling", ":ContentCS:2010:3.2.2")
    .put("Cricket", ":ContentCS:2010:3.2.3.9")
    .put("Concerts", ":ContentCS:2010:3.1.9.14")
    .put("Comedy", ":ContentCS:2010:3.5.7")
    .put("Classic Rock", ":ContentCS:2010:3.6.4.5")
    .put("Children's", ":IntendedAudienceCS:2010:4.2.1")
    .put("Boxing", ":ContentCS:2010:3.2.13.2")
    .put("Beach Volleyball", ":ContentCS:2010:3.2.3.21")
    .put("American Football", ":ContentCS:2010:3.2.3.1")
    .put("Rugby Union", ":ContentCS:2010:3.2.3.19.1")
    .put("Lifestyle", ":ContentCS:2010:3.8")
    .put("Rugby League", ":ContentCS:2010:3.2.3.19.2")
    .put("Romance", ":ContentCS:2010:3.4.3")
    .put("Basketball", ":ContentCS:2010:3.2.3.8")
    .put("Baseball", ":ContentCS:2010:3.2.3.7")
    .put("Athletics", ":ContentCS:2010:3.2.1")
    .put("Archive Football", ":ContentCS:2010:3.2.3.12")
    .put("Karaoke", ":ContentCS:2010:3.6.4.13")
    .put("Action", ":ContentCS:2010:3.4.6")
    .put("Documentary", ":FormatCS:2010:2.1.4")
    .put("Drama", ":ContentCS:2010:3.4")
            .build();
    
    
    public BtVodDescribedFieldsExtractor(
            ImageExtractor imageExtractor,
            TopicCreatingTopicResolver topicCreatingTopicResolver,
            TopicWriter topicWriter
    ) {
        this.imageExtractor = checkNotNull(imageExtractor);
        this.topicCreatingTopicResolver = checkNotNull(topicCreatingTopicResolver);
        this.topicWriter = checkNotNull(topicWriter);
    }
    
    public void setDescribedFieldsFrom(BtVodEntry row, Described described) {
        described.setDescription(row.getDescription());
        described.setLongDescription(row.getProductLongDescription());
        if (row.getProductPriority() != null) {
            described.setPriority(Double.valueOf(row.getProductPriority()));
        }
        
        ImmutableList.Builder<String> genres = ImmutableList.builder();
        for (String btGenre : btGenresFrom(row)) {
            genres.add(
                    String.format(
                            "%s/%s",
                            BT_VOD_GENRE_PREFIX,
                            btGenre
                    )
            );
            if (BT_TO_YOUVIEW_GENRE.containsKey(btGenre)) {
                genres.add(
                        String.format(
                                "%s/%s",
                                YOUVIEW_GENRE_PREFIX,
                                BT_TO_YOUVIEW_GENRE.get(btGenre)
                        )
                );
            }
            described.setGenres(genres.build());
        }

        if (described.getImages() != null 
                && !described.getImages().isEmpty()) {
            described.setImage(Iterables.getFirst(described.getImages(), null).getCanonicalUri());
        }
        
        described.setAliases(aliasesFrom(row));
    }
    
    public Set<String> btGenresFrom(BtVodEntry row) {
        ImmutableSet.Builder<String> genres = ImmutableSet.builder();
        
        if (!Strings.isNullOrEmpty(row.getGenre())) {
            genres.add(row.getGenre());
        }
        
        for (BtVodProductScope scope : row.getProductScopes()) {
            if (scope.getProductMetadata() != null) {
                String subGenres = scope.getProductMetadata().getSubGenres();
                if (!Strings.isNullOrEmpty(subGenres)) {
                    Matcher matcher = SUB_GENRE_ARRAYPATTERN.matcher(subGenres);
                    if (matcher.matches()) {
                        try {
                            CSVParser parse = SUB_GENRE_CSV_PARSER.parse(new StringReader(matcher.group(1)));
                            genres.addAll(parse.getRecords().get(0).iterator());
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }
            }
        }
        return genres.build();
    }
    
    public Iterable<Alias> aliasesFrom(BtVodEntry row) {
        return ImmutableSet.of(
                new Alias(GUID_ALIAS_NAMESPACE, row.getGuid()),
                new Alias(ID_ALIAS_NAMESPACE, row.getId()));
    }

    private Iterable<Image> createImages(BtVodEntry row) {
        // images are of poor quality, so not useful to save
        // return imageExtractor.extractImages(row.getProductImages());
        return ImmutableSet.of(); 
    }

    public synchronized Optional<TopicRef> topicFor(BtVodEntry entry) {
        String contentProviderId = entry.getContentProviderId();
        if (contentProviderId == null) {
            return Optional.absent();
        }
        Topic topic;
        if(topics.containsKey(contentProviderId)) {
            topic = topics.get(contentProviderId);
        } else {

            topic = topicCreatingTopicResolver.topicFor(
                    Publisher.BT_VOD,
                    CONTENT_PROVIDER_TOPIC_NAMESPACE,
                    contentProviderId
            ).requireValue();
            topics.put(contentProviderId, topic);
            topicWriter.write(topic);
        }
        //We do this because TopicCreatingTopicResolver creates topics, but doesn't store it (╯°□°）╯︵ ┻━┻)
        return Optional.of(
                new TopicRef(
                        topic,
                        1.0f,
                        false,
                        TopicRef.Relationship.ABOUT
                )
        );
    }
}
