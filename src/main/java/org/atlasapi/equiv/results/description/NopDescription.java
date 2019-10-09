package org.atlasapi.equiv.results.description;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Implementation which can be used to suppress text when not desired.
 */
public class NopDescription implements ReadableDescription {

    public NopDescription() {

    }

    @Override
    public NopDescription appendText(String format, Object... args) {
        return this;
    }

    @Override
    public ResultDescription startStage(String stageName) {
        return this;
    }

    @Override
    public ResultDescription finishStage() {
        return this;
    }

    @Override
    public List<Object> parts() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return "";
    }
}
