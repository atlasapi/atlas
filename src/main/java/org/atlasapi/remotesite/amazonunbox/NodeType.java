package org.atlasapi.remotesite.amazonunbox;

import com.google.common.base.Optional;


public enum NodeType {
    
    ITEM
    ;
    

    public static Optional<NodeType> typeOf(String typeString) {
        for (NodeType type : NodeType.values()) {
            if (type.name().equalsIgnoreCase(typeString)) {
                return Optional.of(type);
            }
        }
        return Optional.absent();
    }
}
