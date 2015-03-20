package org.atlasapi.remotesite.events;

import static com.google.api.client.util.Preconditions.checkNotNull;

import org.atlasapi.remotesite.DataProcessor;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;


public abstract class EventsIngestTask<S, T, M> extends ScheduledTask {
    
    private final Logger log;
    private final EventsFetcher<S, T, M> fetcher;
    private final DataHandler<S, T, M> dataHandler;
    
    public EventsIngestTask(Logger log, EventsFetcher<S, T, M> fetcher, 
            DataHandler<S, T, M> dataHandler) {
        this.log = checkNotNull(log);
        this.fetcher = checkNotNull(fetcher);
        this.dataHandler = checkNotNull(dataHandler);
    }

    @Override
    protected void runTask() {
        UpdateProgress overallProgress = UpdateProgress.START;
        for (S sport : fetcher.sports()) {
            Optional<? extends EventsData<T, M>> data = fetcher.fetch(sport);
            if (!data.isPresent()) {
                log.error("No data to fetch for sport {}", sport);
            } else {
                overallProgress = overallProgress.reduce(processData(sport, data.get()));
            }
        }
        reportStatus(String.format("Sports processed: %d Results: %s", fetcher.sports().size(), overallProgress.toString()));
    }

    private UpdateProgress processData(S sport, EventsData<T, M> data) {
        DataProcessor<T> teamProcessor = teamProcessor(sport);
        for (T team : data.teams()) {
            teamProcessor.process(team);
        }

        String teamResult = "Teams: " + teamProcessor.getResult().toString();
        reportStatus(sport.toString() + ": " + teamResult);
        
        DataProcessor<M> matchProcessor = matchProcessor(sport);
        for (M match : data.matches()) {
            matchProcessor.process(match);
        }
        
        String eventResult = "Events: " + matchProcessor.getResult().toString();
        reportStatus(sport.toString() + ": " + teamResult + " " + eventResult);
        return teamProcessor.getResult().reduce(matchProcessor.getResult());
    }

    private DataProcessor<T> teamProcessor(final S sport) {
        return new DataProcessor<T>() {
            
            UpdateProgress progress = UpdateProgress.START;

            @Override
            public boolean process(T team) {
                try {
                    dataHandler.handleTeam(team, sport);
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.warn("Error processing team: " + team, e);
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
                reportStatus(progress.toString());
                return shouldContinue();
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }
    
    private DataProcessor<M> matchProcessor(final S sport) {
        return new DataProcessor<M>() {
            
            UpdateProgress progress = UpdateProgress.START;

            @Override
            public boolean process(M match) {
                try {
                    dataHandler.handleMatch(match, sport);
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.warn("Error processing team: " + match, e);
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
                reportStatus(progress.toString());
                return shouldContinue();
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }
}
