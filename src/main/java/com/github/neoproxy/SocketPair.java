package com.github.neoproxy;

import org.apache.commons.io.IOUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Daneel Yaitskov
 */
public class SocketPair {
    public final ByteBuffer buffer;
    public final SocketChannel src;
    public final SocketChannel dst;
    public final Status status;

    public SocketPair(ByteBuffer buffer, SocketChannel src, SocketChannel dst, Status status) {
        this.buffer = buffer;
        this.src = src;
        this.dst = dst;
        this.status = status;
    }

    public void close() {
        IOUtils.closeQuietly(src);
    }
}
