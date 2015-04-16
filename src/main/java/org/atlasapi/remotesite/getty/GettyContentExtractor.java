package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.media.entity.Publisher.KM_GETTY;
import static org.joda.time.DateTimeConstants.SECONDS_PER_HOUR;
import static org.joda.time.DateTimeConstants.SECONDS_PER_MINUTE;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

public class GettyContentExtractor implements ContentExtractor<VideoResponse, Content> {

    private static final String GETTY_URI_PATTERN = "http://gettyimages.co.uk/%s";
    private static final String GETTY_CURIE_PATTERN = "getty:%s";
    
    private final Splitter durationSplitter = Splitter.on(":").omitEmptyStrings();
    private final Splitter dateSplitter = Splitter.onPattern("[-|+]").omitEmptyStrings();
    
    private final TopicGuesser topicGuesser;
    
    public GettyContentExtractor(TopicGuesser topicGuesser) {
        this.topicGuesser = checkNotNull(topicGuesser);
    }
    
    @Override
    public Content extract(VideoResponse source) {
        return extractItem(source);
    }
    
    private Item extractItem(VideoResponse source) {
        Item item = new Item();
        
        item.setVersions(extractVersions(source));
        item.setFirstSeen(extractDate(source.getDateCreated()));
        item.setDescription(source.getDescription());
        item.setTitle(source.getTitle());
        item.setPublisher(KM_GETTY);
        item.setCanonicalUri(uri(source.getAssetId()));
        item.setCurie(curie(source.getAssetId()));
        item.setLastUpdated(new DateTime(DateTimeZone.UTC));
        item.setKeyPhrases(keyphrases(source.getKeywords()));
        item.setMediaType(MediaType.VIDEO);
        item.setImage(source.getThumb());
        item.setThumbnail(source.getThumb());
//        item.setTopicRefs(topicGuesser.guessTopics(source.getKeywords()));

        return item;
    }

    private Iterable<KeyPhrase> keyphrases(List<String> keywords) {
        Builder<KeyPhrase> keyphrases = new Builder<KeyPhrase>();
        for (String keyword : keywords) {
            keyphrases.add(new KeyPhrase(keyword, KM_GETTY));
        }
        return keyphrases.build();
    }

    private Set<Version> extractVersions(VideoResponse video) {
        Duration duration = extractDuration(video.getDuration());
        List<String> aspectRatios = video.getAspectRatios();
        if (!aspectRatios.isEmpty()) {
            return addAspectRationAndDuration(duration, aspectRatios);
        }
        Version version = new Version();
        version.setDuration(duration);
        return ImmutableSet.of(version);
    }
    
    private Set<Version> addAspectRationAndDuration(Duration duration, List<String> aspectRatios) {
        com.google.common.collect.ImmutableSet.Builder<Version> versions = new ImmutableSet.Builder<Version>();
        if (aspectRatios.isEmpty()) {
            Version version = new Version();
            Encoding encoding = new Encoding();
            encoding.setAvailableAt(ImmutableSet.of(new Location()));
            version.addManifestedAs(encoding);
            version.setDuration(duration);
            versions.add(version);
        } else {
            for (String aspectRatio : aspectRatios) {
                Version version = new Version();
                Encoding encoding = new Encoding();
                encoding.setAvailableAt(ImmutableSet.of(new Location()));
                encoding.setVideoAspectRatio(aspectRatio);
                version.addManifestedAs(encoding);
                version.setDuration(duration);
                versions.add(version);
            }
        }
        return versions.build();
    }

    private Duration extractDuration(String duration) {
        //duration is of type hh:mm:ss:millis
        List<String> tokens = ImmutableList.copyOf(durationSplitter.split(duration));
        int hours = Integer.parseInt(tokens.get(0));
        int minutes = Integer.parseInt(tokens.get(1));
        int seconds = Integer.parseInt(tokens.get(2));
        return Duration.standardSeconds(seconds + minutes * SECONDS_PER_MINUTE + hours * SECONDS_PER_HOUR);
    }

    private DateTime extractDate(String date) {
        // date example: /Date(1406012400000-0700)/
        String shorterDate = date.replace("/Date(", "");
        List<String> splitDate = ImmutableList.copyOf(dateSplitter.split(shorterDate));
        return new DateTime(Long.valueOf(splitDate.get(0)), DateTimeZone.UTC);
    }

    static String uri(String id) {
        return String.format(GETTY_URI_PATTERN, id);
    }
    
    static String curie(String id) {
        String curie = String.format(GETTY_CURIE_PATTERN, id);
        return curie;
    }

}
