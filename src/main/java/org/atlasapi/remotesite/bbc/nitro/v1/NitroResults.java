package org.atlasapi.remotesite.bbc.nitro.v1;

import com.google.api.client.util.Key;

import java.util.List;

public class NitroResults<T> {

    @Key private int total;
    @Key private List<T> items;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    @Override
    public String toString() {
        return items.toString();
    }
    
}
