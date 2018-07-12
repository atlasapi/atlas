package org.atlasapi.util;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This queue will always block after reaching its capacity. This is because the default executor
 * uses offer(e) to add stuff to queue, so by default all blocking queues will overflow instead of
 * block. It was easier to mess with the queue than the executor.
 * @param <E>
 */
public class AlwaysBlockingQueue<E> extends LinkedBlockingQueue<E> {

    public AlwaysBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(E e) {
        try {
            super.put(e);
            return true;
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex); //because we cannot throw checked exceptions since the method is overridden.
        }
    }
}
