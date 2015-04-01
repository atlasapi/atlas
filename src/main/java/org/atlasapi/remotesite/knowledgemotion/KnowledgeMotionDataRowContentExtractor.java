package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.intl.Countries;

public class KnowledgeMotionDataRowContentExtractor implements ContentExtractor<KnowledgeMotionDataRow, Optional<? extends Content>> {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionDataRowContentExtractor.class);
    private static final Function<String, String> PRICE_CATEGORY_TO_GENRE = new Function<String, String>() {
        @Override
        public String apply(String priceCategory) {
            try {
                return String.format(
                        "%s%s",
                        KNOWLEDGEMOTION_GENRE_PREFIX,
                        URLEncoder.encode(priceCategory, "UTF-8")
                );
            } catch (UnsupportedEncodingException e) {
                //this should never happen
                log.error("Error encoding price category", e);
                return null;
            }
        }
    };

    private static final String KNOWLEDGEMOTION_GENRE_PREFIX = "http://knowledgemotion.com/";

    private final Splitter idSplitter = Splitter.on(":").omitEmptyStrings();
    private final PeriodFormatter durationFormatter = new PeriodFormatterBuilder()
        .appendHours().minimumPrintedDigits(2)
        .appendSeparator(":")
        .appendMinutes().minimumPrintedDigits(2)
        .appendSeparator(":")
        .appendSeconds().minimumPrintedDigits(2)
        .appendSeparator(";")
        .appendMillis().minimumPrintedDigits(2)
        .toFormatter();
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    private final ImmutableMap<String, KnowledgeMotionSourceConfig> sources;
    private final TopicGuesser topicGuesser;

    public KnowledgeMotionDataRowContentExtractor(Iterable<KnowledgeMotionSourceConfig> sources, TopicGuesser topicGuesser) {
        ImmutableMap.Builder<String, KnowledgeMotionSourceConfig> sourceMap = ImmutableMap.builder();
        for (KnowledgeMotionSourceConfig source : sources) {
            sourceMap.put(source.rowHeader(), source);
        }
        this.sources = sourceMap.build();
        this.topicGuesser = checkNotNull(topicGuesser);
    }

    @Override
    public Optional<? extends Content> extract(KnowledgeMotionDataRow source) {
        return extractItem(source);
    }

    private Optional<Item> extractItem(KnowledgeMotionDataRow dataRow) {
        KnowledgeMotionSourceConfig sourceConfig = sources.get(dataRow.getSource());
        if (sourceConfig == null) {
            return Optional.absent();
        }

        Item item = new Item();

        String id = Iterables.getLast(idSplitter.split(dataRow.getId()));
        Publisher publisher = sourceConfig.publisher();

        ReleaseDate releaseDate = new ReleaseDate(extractDate(dataRow.getDate()).toLocalDate(),
            Countries.ALL, ReleaseDate.ReleaseType.GENERAL);

        item.setVersions(extractVersions(dataRow.getDuration(), dataRow.getTermsOfUse()));
        item.setReleaseDates(Lists.newArrayList(releaseDate));
        item.setDescription(dataRow.getDescription());
        item.setTitle(dataRow.getTitle());
        item.setPublisher(publisher);
        item.setCanonicalUri(sourceConfig.uri(id));
        item.setCurie(sourceConfig.curie(id));
        item.setLastUpdated(new DateTime(DateTimeZone.UTC));
        item.setMediaType(MediaType.VIDEO);
        item.setGenres(Iterables.transform(dataRow.getPriceCategories(), PRICE_CATEGORY_TO_GENRE));

        List<String> keyPhrases = dataRow.getKeywords();
        item.setKeyPhrases(keyphrases(keyPhrases, publisher));
        item.setTopicRefs(topicGuesser.guessTopics(keyPhrases));

        return Optional.of(item);
    }

    private Iterable<KeyPhrase> keyphrases(List<String> keywords, Publisher publisher) {
        Builder<KeyPhrase> keyphrases = new ImmutableList.Builder<KeyPhrase>();
        for (String keyword : keywords) {
            keyphrases.add(new KeyPhrase(keyword, publisher));
        }
        return keyphrases.build();
    }

    private Set<Version> extractVersions(String duration, Optional<String> termsOfUse) {
        Version version = new Version();
        Encoding encoding = new Encoding();
        Location location = new Location();
        if(termsOfUse.isPresent()) {
            Policy policy = new Policy();
            policy.setTermsOfUse(termsOfUse.get());
            location.setPolicy(policy);
        }
        encoding.setAvailableAt(ImmutableSet.of(location));
        version.addManifestedAs(encoding);
        version.setDuration(extractDuration(duration));
        return ImmutableSet.of(version);
    }

    private Duration extractDuration(String duration) {
        //duration is of type hh:mm:ss;f
        return durationFormatter.parsePeriod(duration).toStandardDuration();
    }

    private DateTime extractDate(String date) {
        return dateTimeFormatter.parseDateTime(date).withZone(DateTimeZone.UTC);
    }


}
