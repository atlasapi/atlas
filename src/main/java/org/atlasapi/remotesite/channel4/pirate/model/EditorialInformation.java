package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class EditorialInformation {

    private final Brand brand;
    private final String contractNumber;
    private final Episode episode;
    private final Genre genre;
    private final String programmeNumber;
    private final Series series;

    @JsonCreator
    public EditorialInformation(
            @JsonProperty("Brand") Brand brand,
            @JsonProperty("ContractNumber") String contractNumber,
            @JsonProperty("Episode") Episode episode,
            @JsonProperty("Genre") Genre genre,
            @JsonProperty("ProgrammeNumber")String programmeNumber,
            @JsonProperty("Series") Series series
    ) {
        this.brand = checkNotNull(brand);
        this.contractNumber = checkNotNull(contractNumber);
        this.episode = checkNotNull(episode);
        this.genre = checkNotNull(genre);
        this.programmeNumber = checkNotNull(programmeNumber);
        this.series = checkNotNull(series);
    }

    public Brand getBrand() {
        return brand;
    }

    public String getContractNumber() {
        return contractNumber;
    }

    public Episode getEpisode() {
        return episode;
    }

    public Genre getGenre() {
        return genre;
    }

    public String getProgrammeNumber() {
        return programmeNumber;
    }

    public Series getSeries() {
        return series;
    }
}
