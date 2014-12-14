package com.github.neoproxy;

/**
 * Daneel Yaitskov
 */
public class Status {
    private boolean closing;
    private int counter;

    public void inc() {
        if (++counter == 0) {
            ++counter;
        }
    }

    public int reset() {
        final int was = counter;
        counter = 0;
        return was;
    }

    public boolean isClosing() {
        return closing;
    }

    public void markClosing() {
        closing = true;
    }
}
