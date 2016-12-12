package org.atlasapi.query.content.merge;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.gdata.util.common.base.Preconditions.checkNotNull;

public class ContentMerger {

    private final ItemMerger itemMerger;
    private final EpisodeMerger episodeMerger;

    private ContentMerger(
            ItemMerger itemMerger,
            EpisodeMerger episodeMerger
    ){
        this.itemMerger = checkNotNull(itemMerger);
        this.episodeMerger = checkNotNull(episodeMerger);
    }

    public static ContentMerger create(
            ItemMerger itemMerger,
            EpisodeMerger episodeMerger
    ){
        return new ContentMerger(
                itemMerger,
                episodeMerger
        );
    }

    public Content merge(
            Maybe<Identified> possibleExisting,
            Content update,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ) {
        if (possibleExisting.isNothing()) {
            return update;
        }
        Identified identifiedContent = possibleExisting.requireValue();

        if (identifiedContent instanceof Content) {
            Content existingContent = (Content) identifiedContent;

            if (!existingContent.getClass().equals(update.getClass())){
                return mergeWithTypeUpdate(existingContent, update, merge, defaultBroadcastMerger);
            }
            return merge(existingContent, update, merge, defaultBroadcastMerger);
        }
        throw new IllegalStateException("Entity for " + update.getCanonicalUri() + " not Content");
    }

    private Content mergeWithTypeUpdate(
            Content existing,
            Content update,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        if (update instanceof Episode){
            return merge(
                    copyToEpisode(existing, merge, defaultBroadcastMerger),
                    update,
                    merge,
                    defaultBroadcastMerger
            );
        }
        if (update instanceof Film){
            return merge(
                    copyToFilm(existing, merge, defaultBroadcastMerger),
                    update,
                    merge,
                    defaultBroadcastMerger
            );
        }
        if (update instanceof Brand){
            return merge(
                    copyToBrand(existing, merge, defaultBroadcastMerger),
                    update,
                    merge,
                    defaultBroadcastMerger
            );
        }
        if (update instanceof Series){
            return merge(
                    copyToSeries(existing, merge, defaultBroadcastMerger),
                    update,
                    merge,
                    defaultBroadcastMerger
            );
        }
        if (update instanceof Item){
            return merge(
                    copyToItem(existing, merge, defaultBroadcastMerger),
                    update,
                    merge,
                    defaultBroadcastMerger
            );
        }

        return merge(existing, update, merge, defaultBroadcastMerger);
    }

    private Content copyToBrand(
            Content existing,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        Brand brand = new Brand();
        Brand.copyTo(existing, brand);
        return merge(brand, existing, merge, defaultBroadcastMerger);
    }

    private Content copyToEpisode(
            Content existing,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        Episode episode = new Episode();
        Episode.copyTo(existing, episode);
        return merge(episode, existing, merge, defaultBroadcastMerger);
    }

    private Content copyToSeries(
            Content existing,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        Series series = new Series();
        Series.copyTo(existing, series);
        return merge(series, existing, merge, defaultBroadcastMerger);
    }

    private Content copyToFilm(
            Content existing,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        Film film = new Film();
        Film.copyTo(existing, film);
        return merge(film, existing, merge, defaultBroadcastMerger);
    }

    private Content copyToItem(
            Content existing,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ){
        Item item = new Item();
        Item.copyTo(existing, item);
        return merge(item, existing, merge, defaultBroadcastMerger);
    }

    private Content merge(
            Content existing,
            Content update,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ) {
        existing.setActivelyPublished(update.isActivelyPublished());

        existing.setEquivalentTo(
                merge
                ? merge(existing.getEquivalentTo(), update.getEquivalentTo())
                : update.getEquivalentTo()
        );
        existing.setLastUpdated(update.getLastUpdated());
        existing.setTitle(update.getTitle());
        existing.setShortDescription(update.getShortDescription());
        existing.setMediumDescription(update.getMediumDescription());
        existing.setLongDescription(update.getLongDescription());
        existing.setDescription(update.getDescription());
        existing.setImage(update.getImage());
        existing.setThumbnail(update.getThumbnail());
        existing.setMediaType(update.getMediaType());
        existing.setSpecialization(update.getSpecialization());
        existing.setRelatedLinks(
                merge
                ? merge(existing.getRelatedLinks(), update.getRelatedLinks())
                : update.getRelatedLinks()
        );
        existing.setAliases(
                merge
                ? merge(existing.getAliases(), update.getAliases())
                : update.getAliases()
        );
        existing.setTopicRefs(
                merge ?
                merge(existing.getTopicRefs(), update.getTopicRefs()) :
                update.getTopicRefs()
        );
        existing.setPeople(
                merge
                ? merge(existing.people(), update.people())
                : update.people()
        );
        existing.setKeyPhrases(update.getKeyPhrases());
        existing.setClips(
                merge
                ? merge(existing.getClips(), update.getClips())
                : update.getClips()
        );
        existing.setPriority(update.getPriority());
        existing.setEventRefs(
                merge
                ? merge(existing.events(), update.events())
                : update.events()
        );
        existing.setImages(
                merge
                ? merge(existing.getImages(), update.getImages())
                : update.getImages()
        );
        existing.setAwards(
                merge
                ? merge(existing.getAwards(), update.getAwards())
                : update.getAwards());
        existing.setPresentationChannel(update.getPresentationChannel());
        existing.setYear(update.getYear());

        if (existing instanceof Episode && update instanceof Episode) {
            episodeMerger.mergeEpisodes((Episode) existing, (Episode) update);
        }
        if (existing instanceof Item && update instanceof Item) {
            return itemMerger.mergeItems(
                    itemMerger.mergeReleaseDates((Item) existing, (Item) update, merge),
                    (Item) update,
                    merge,
                    defaultBroadcastMerger
            );
        }
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }

    private <T> List<T> merge(List<T> existing, List<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing)).asList();
    }
}
