package org.atlasapi.remotesite.itunes.epf;

import static org.atlasapi.remotesite.itunes.epf.EpfHelper.uriForEpisode;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.itunes.epf.model.EpfVideo;
import org.joda.time.Duration;
import com.google.common.base.Optional;

public class ItunesVideoEpisodeExtractor implements ContentExtractor<ItunesEpfVideoSource, Episode> {

    @Override
    public Episode extract(ItunesEpfVideoSource source) {
        
        EpfVideo row = source.video();
        
        Integer videoId = row.get(EpfVideo.VIDEO_ID);
        Episode episode = new Episode(uriForEpisode(videoId), EpfHelper.uriForEpisode(videoId), Publisher.ITUNES);
        episode.setTitle(row.get(EpfVideo.NAME));
        episode.setDescription(row.get(EpfVideo.LONG_DESCRIPTION));
        episode.setThumbnail(row.get(EpfVideo.ARTWORK_URL));
        episode.setImage(row.get(EpfVideo.ARTWORK_URL).replace("133x100-99.jpg", "227x170-99.jpg"));
        episode.setIsLongForm(true);
        episode.setMediaType(MediaType.VIDEO);
        episode.setSpecialization(Specialization.TV);
        
        Version version = new Version();
        Optional<Integer> maybeDuration = Optional.of(row.get(EpfVideo.TRACK_LENGTH));

        if (maybeDuration.isPresent()) {
            version.setDuration(new Duration(maybeDuration.get().longValue()));
        }
        
        Iterable<Location> locations = source.locations();
        if(locations != null) {
            Encoding encoding = new Encoding();
            for (Location location : locations) {
                location.getPolicy().setAvailabilityStart(row.get(EpfVideo.ITUNES_RELEASE_DATE));
                encoding.addAvailableAt(location);
            }
            version.addManifestedAs(encoding);
        }
        
        episode.addVersion(version);
        return episode;
    }

}
