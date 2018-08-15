package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

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
        this.brand = brand;
        this.contractNumber = contractNumber;
        this.episode = episode;
        this.genre = genre;
        this.programmeNumber = programmeNumber;
        this.series = series;
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

    @Nullable
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
