package org.atlasapi.deer.elasticsearch.content;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Alias implements Hashable {

    public static final String URI_NAMESPACE = "uri";

    private final String namespace;
    private final String value;

    public Alias(String namespace, String value) {
        this.namespace = checkNotNull(namespace);
        this.value = checkNotNull(value);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return namespace.hashCode() ^ value.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Alias) {
            Alias other = (Alias) that;
            return namespace.equals(other.namespace)
                    && value.equals(other.value);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("namespace", namespace)
                .add("value", value)
                .toString();
    }
}
