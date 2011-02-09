package org.atlasapi.remotesite.pa;

import java.util.Set;

import org.atlasapi.genres.GenreMap;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.remotesite.ContentWriters;
import org.atlasapi.remotesite.pa.bindings.Billing;
import org.atlasapi.remotesite.pa.bindings.Category;
import org.atlasapi.remotesite.pa.bindings.ChannelData;
import org.atlasapi.remotesite.pa.bindings.PictureUsage;
import org.atlasapi.remotesite.pa.bindings.ProgData;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.internal.Sets;
import com.metabroadcast.common.base.Maybe;

public class PaProgrammeProcessor {
    
    private static final String PA_BASE_URL = "http://pressassociation.com";
    private static final String PA_BASE_IMAGE_URL = "http://images.atlasapi.org/pa/";
    private static final String BROADCAST_ID_PREFIX = "pa:";
    private static final String YES = "yes";
    
    private final ContentWriters contentWriter;
    private final ContentResolver contentResolver;
    private final AdapterLog log;
    
    private final PaChannelMap channelMap = new PaChannelMap();
    private final GenreMap genreMap = new PaGenreMap();

    public PaProgrammeProcessor(ContentWriters contentWriter, ContentResolver contentResolver, AdapterLog log) {
        this.contentWriter = contentWriter;
        this.contentResolver = contentResolver;
        this.log = log;
    }

    public void process(ProgData progData, ChannelData channelData, DateTimeZone zone) {
        try {
            Maybe<Brand> brand = Maybe.nothing();
            try {
                brand = getBrand(progData);
            } catch (ClassCastException e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription("This is definitely where the class cast will happen, with the brand " + e.getMessage()));
            }
            Maybe<Series> series = Maybe.nothing();
            try {
                series = getSeries(progData);
            } catch (ClassCastException e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription("This is definitely where the class cast will happen, with the series " + e.getMessage()));
            }
            Maybe<Episode> episode = Maybe.nothing();
            try {
                episode = getEpisode(progData, channelData, zone);
            } catch (ClassCastException e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription("This is definitely where the class cast will happen, with the episode " + e.getMessage()));
            }

            if (episode.hasValue()) {
                if (series.hasValue()) {
                    series.requireValue().addItem(episode.requireValue());
                    contentWriter.createOrUpdateDefinitivePlaylist(series.requireValue());
                }
                try {
                if (brand.hasValue()) {
                    brand.requireValue().addItem(episode.requireValue());
                    contentWriter.createOrUpdateDefinitivePlaylist(brand.requireValue());
                } else {
                    contentWriter.createOrUpdateDefinitiveItem(episode.requireValue());
                }
                } catch (ClassCastException e) {
                    log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription("This is definitely where the class cast will happen, when it's persisted " + e.getMessage()));
                }
            }
        } catch (Exception e) {
            log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription(e.getMessage()));
        }
    }

    private Maybe<Brand> getBrand(ProgData progData) {
        String brandId = progData.getSeriesId();
        if (Strings.isNullOrEmpty(brandId) || Strings.isNullOrEmpty(brandId.trim())) {
            return Maybe.nothing();
        }

        String brandUri = PA_BASE_URL + "/brands/" + brandId;
        Content resolvedContent = contentResolver.findByUri(brandUri);
        Brand brand;
        if (resolvedContent instanceof Brand) {
            brand = (Brand) resolvedContent;
        } else {
            brand = new Brand(brandUri, "pa:b-" + brandId, Publisher.PA);
        }
        
        brand.setTitle(progData.getTitle());
        brand.setSpecialization(specialization(progData));
        brand.setGenres(genreMap.map(ImmutableSet.copyOf(Iterables.transform(progData.getCategory(), Category.TO_GENRE_URIS))));

        if (progData.getPictures() != null) {
            for (PictureUsage picture : progData.getPictures().getPictureUsage()) {
                if (picture.getType().equals("season") && brand.getImage() == null){
                    brand.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                }
                if (picture.getType().equals("series")){
                    brand.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                    break;
                }
            }
        }

        return Maybe.just(brand);
    }

    private Maybe<Series> getSeries(ProgData progData) {
        if (Strings.isNullOrEmpty(progData.getSeriesNumber()) || Strings.isNullOrEmpty(progData.getSeriesId())) {
            return Maybe.nothing();
        }
        
        String seriesUri = PA_BASE_URL + "/series/" + progData.getSeriesId() + "-" + progData.getSeriesNumber();
        
        Content resolvedContent = contentResolver.findByUri(seriesUri);
        Series series;
        if (resolvedContent instanceof Series) {
            series = (Series) resolvedContent;
        } else {
            series = new Series(seriesUri, "pa:s-" + progData.getSeriesId() + "-" + progData.getSeriesNumber());
        }
        
        series.setPublisher(Publisher.PA);
        series.setSpecialization(specialization(progData));
        series.setGenres(genreMap.map(ImmutableSet.copyOf(Iterables.transform(progData.getCategory(), Category.TO_GENRE_URIS))));
        
        if (progData.getPictures() != null) {
            for (PictureUsage picture : progData.getPictures().getPictureUsage()) {
                if (picture.getType().equals("series") && series.getImage() == null){
                    series.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                }
                if (picture.getType().equals("season")){
                    series.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                    break;
                }
            }
        }

        return Maybe.just(series);
    }

    private Maybe<Episode> getEpisode(ProgData progData, ChannelData channelData, DateTimeZone zone) {
        String channelUri = channelMap.getChannelUri(Integer.valueOf(channelData.getChannelId()));
        if (Strings.isNullOrEmpty(channelUri)) {
            return Maybe.nothing();
        }

        String episodeUri = PA_BASE_URL + "/episodes/" + programmeId(progData);
        Content resolvedContent = contentResolver.findByUri(episodeUri);
        Episode episode;
        if (resolvedContent instanceof Episode) {
            episode = (Episode) resolvedContent;
        } else {
            episode = getBasicEpisode(progData);
        }
        
        if (progData.getEpisodeTitle() != null) {
            episode.setTitle(progData.getEpisodeTitle());
        } else {
            episode.setTitle(progData.getTitle());
        }

        try {
            if (progData.getEpisodeNumber() != null) {
                episode.setEpisodeNumber(Integer.valueOf(progData.getEpisodeNumber()));
            }
            if (progData.getSeriesNumber() != null) {
                episode.setSeriesNumber(Integer.valueOf(progData.getSeriesNumber()));
            }
        } catch (NumberFormatException e) {
            // sometimes we don't get valid numbers
        }

        if (progData.getBillings() != null) {
            for (Billing billing : progData.getBillings().getBilling()) {
                if (billing.getType().equals("synopsis")) {
                    episode.setDescription(billing.getvalue());
                    break;
                }
            }
        }
        
        episode.setMediaType(MediaType.VIDEO);
        episode.setSpecialization(specialization(progData));
        episode.setGenres(genreMap.map(ImmutableSet.copyOf(Iterables.transform(progData.getCategory(), Category.TO_GENRE_URIS))));

        if (progData.getPictures() != null) {
            for (PictureUsage picture : progData.getPictures().getPictureUsage()) {
                if (picture.getType().equals("series") && episode.getImage() == null){
                    episode.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                }
                if (picture.getType().equals("season") && episode.getImage() == null){
                    episode.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                }
                if (picture.getType().equals("episode")){
                    episode.setImage(PA_BASE_IMAGE_URL + picture.getvalue());
                    break;
                }
            }
        }


        Version version = findBestVersion(episode.getVersions());

        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));

        DateTime transmissionTime = getTransmissionTime(progData.getDate(), progData.getTime(), zone);
        Broadcast broadcast = new Broadcast(channelUri, transmissionTime, duration).withId(BROADCAST_ID_PREFIX+progData.getShowingId());
        addBroadcast(version, broadcast);

        return Maybe.just(episode);
    }
    
    private void addBroadcast(Version version, Broadcast broadcast) {
        if (! Strings.isNullOrEmpty(broadcast.getId())) {
            Set<Broadcast> broadcasts = Sets.newHashSet();
            
            for (Broadcast currentBroadcast: version.getBroadcasts()) {
                if ((! Strings.isNullOrEmpty(currentBroadcast.getId()) && ! broadcast.getId().equals(currentBroadcast.getId())) ||
                     ! (currentBroadcast.getBroadcastOn().equals(broadcast.getBroadcastOn()) && currentBroadcast.getTransmissionTime().equals(broadcast.getTransmissionTime()) && currentBroadcast.getTransmissionEndTime().equals(broadcast.getTransmissionEndTime()))) {
                    broadcasts.add(currentBroadcast);
                }
            }
            broadcasts.add(broadcast);
            
            version.setBroadcasts(broadcasts);
        }
    }

    private Version findBestVersion(Iterable<Version> versions) {
        for (Version version : versions) {
            if (version.getProvider() == Publisher.PA) {
                return version;
            }
        }

        return versions.iterator().next();
    }

    private Episode getBasicEpisode(ProgData progData) {
        Episode episode = new Episode(PA_BASE_URL + "/episodes/" + programmeId(progData), "pa:e-" + programmeId(progData), Publisher.PA);

        Version version = new Version();
        version.setProvider(Publisher.PA);
        episode.addVersion(version);

        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));
        version.setDuration(duration);

        episode.addVersion(version);

        return episode;
    }
    
    protected static Specialization specialization(ProgData progData) {
        return YES.equals(progData.getAttr().getFilm()) ? Specialization.FILM : Specialization.TV;
    }

    protected static DateTime getTransmissionTime(String date, String time, DateTimeZone zone) {
        String dateString = date + "-" + time;
        return DateTimeFormat.forPattern("dd/MM/yyyy-HH:mm").withZone(zone).parseDateTime(dateString);
    }
    
    protected static String programmeId(ProgData progData) {
        return ! Strings.isNullOrEmpty(progData.getRtFilmnumber()) ? progData.getRtFilmnumber() : progData.getProgId();
    }
}
