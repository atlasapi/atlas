package org.atlasapi.remotesite.btvod.portal;

import java.util.Set;

import com.google.common.base.Optional;


public interface PortalClient {

    /**
     * Provide the members of a given groupId. Will return 
     * {@link Optional.absent()} in the case of the group not 
     * being present. The group will not be present in two cases:
     * 
     *  1) If the group is not set up
     *  2) If there are no entries in a valid group
     * 
     * @param groupId
     * @return
     */
    Optional<Set<String>> getProductIdsForGroup(String groupId);
}
