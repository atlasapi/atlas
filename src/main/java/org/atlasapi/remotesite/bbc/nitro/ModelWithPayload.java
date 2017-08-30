package org.atlasapi.remotesite.bbc.nitro;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.gdata.util.common.base.Preconditions.checkNotNull;

public class ModelWithPayload<T> {

    private final T model;
    private final Object payload;

    public ModelWithPayload(@Nonnull T model, @Nullable Object payload) {
        this.model = checkNotNull(model);
        this.payload = payload;
    }

    public T getModel() {
        return model;
    }

    @Nullable
    public Object getPayload() {
        return payload;
    }

    @SuppressWarnings("unchecked")
    public <U> ModelWithPayload<U> asModelType(Class<U> type) throws ClassCastException {
        type.cast(this.model);
        return (ModelWithPayload<U>) this;
    }

    @Override
    public boolean equals(Object o) {
        return getModel().equals(o);
    }

    @Override
    public int hashCode() {
        return getModel().hashCode();
    }

    @Override
    public String toString() {
        return "ModelWithPayload{" +
               " model=" + model +
               ", payload=" + payload +
               '}';
    }
}
