package org.atlasapi.equiv.results.description;

import java.util.List;
import java.util.Stack;

import com.google.common.collect.Lists;

public class DefaultDescription implements ReadableDescription {

    private final List<Object> parts;
    private List<Object> currentPart;
    private Stack<List<Object>> parentParts;

    public DefaultDescription() {
        this.parts = Lists.newArrayList();
        this.currentPart = parts;
        this.parentParts = new Stack<List<Object>>();
    }
    
    @Override
    public DefaultDescription appendText(String format, Object... args) {
        currentPart.add(String.format(format, args));
        return this;
    }

    @Override
    public ResultDescription startStage(String stageName) {
        currentPart.add(stageName);
        List<Object> newPart = Lists.newLinkedList();
        currentPart.add(newPart);
        parentParts.push(currentPart);
        currentPart = newPart;
        return this;
    }

    @Override
    public ResultDescription finishStage() {
        if(parentParts.isEmpty()) {
            this.currentPart = parts;
        } else {
            this.currentPart = parentParts.pop();
        }
        return this;
    }
    
    public List<Object> parts() {
        return parts;
    }
    
    @Override
    public String toString() {
        return parts.toString();
    }
}
