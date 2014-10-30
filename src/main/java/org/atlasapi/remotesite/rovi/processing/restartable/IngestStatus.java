package org.atlasapi.remotesite.rovi.processing.restartable;

import java.util.Objects;

public class IngestStatus {

    private final IngestStep currentStep;
    private final long processedLine;

    public IngestStatus(IngestStep currentStep, long processedLine) {
        this.currentStep = currentStep;
        this.processedLine = processedLine;
    }

    public IngestStep getCurrentStep() {
        return currentStep;
    }

    public long getProcessedLine() {
        return processedLine;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (!(that instanceof IngestStatus)) {
            return false;
        }

        IngestStatus thatStatus = (IngestStatus) that;

        return Objects.equals(this.currentStep, thatStatus.currentStep)
                && Objects.equals(this.processedLine, thatStatus.processedLine);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentStep, processedLine);
    }
}
