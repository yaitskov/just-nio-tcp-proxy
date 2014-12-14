package com.github.neoproxy;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
* Daneel Yaitskov
*/
class TcpProxy implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TcpProxy.class);

    private final long timeout;
    private final SocketAddress target;
    private final Selector selector;
    private final ServerSocketChannel ssChannel;
    private final ServerSocket serverSocket;
    private final Map<SocketChannel, SocketPair> pairsRead = new HashMap<>();
    private final Map<SocketChannel, SocketPair> pairsFlush = new HashMap<>();
    private final int bufferSize;

    public TcpProxy(int localPort, long timeout,
                    SocketAddress target, int bufferSize)
            throws IOException {
        this.timeout = timeout;
        this.target = target;
        this.bufferSize = bufferSize;
        selector = Selector.open();
        try {
            ssChannel = ServerSocketChannel.open();
            try {
                ssChannel.configureBlocking(false);
                serverSocket = ssChannel.socket();
                try {
                    serverSocket.bind(new InetSocketAddress(localPort));
                    ssChannel.register(selector, SelectionKey.OP_ACCEPT);
                } catch (IOException e) {
                    IOUtils.closeQuietly(serverSocket);
                    throw e;
                }
            } catch (IOException e) {
                IOUtils.closeQuietly(ssChannel);
                throw e;
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(selector);
            throw e;
        }
    }

    public void run() {
        logger.debug("Proxy on port {} started.", serverSocket.getLocalPort());
        try {
            while (selector.isOpen()) {
                final int ready = selector.select(timeout);
                if (ready == 0) {
                    closeIdlePairs();
                }
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    dispatch(key);
                    iterator.remove();
                }
            }
        } catch (ClosedSelectorException e) {
            // break
        } catch (Exception e) {
            logger.error("Proxy failed.", e);
        } finally {
            close();
            for (SocketPair pair : pairsRead.values()) {
                pair.close();
            }
            pairsRead.clear();
            logger.debug("Proxy on port {} ended.", serverSocket.getLocalPort());
        }
    }

    public void close() {
        IOUtils.closeQuietly(serverSocket);
        IOUtils.closeQuietly(ssChannel);
        IOUtils.closeQuietly(selector);
    }

    private void closeIdlePairs() {
        Collection<SocketPair> closing = pairsRead.values().stream()
                .filter(pair -> pair.status.reset() == 0)
                .collect(Collectors.toMap(pair -> pair.status,
                        Function.identity()))
                .values();
        closing.forEach(this::closePair);
        if (closing.isEmpty()) {
            return;
        }
        logger.debug("Close {} pairs by timeout.", closing.size());
    }

    private void dispatch(SelectionKey key) {
        if (key.isValid()) {
            if (key.isAcceptable()) {
                accept(key);
            } else if (key.isReadable()) {
                readFrom((SocketChannel) key.channel());
            } else if (key.isWritable()) {
                flushBuffer((SocketChannel) key.channel());
            } else {
                logger.info("Unsupported key {}.", key.readyOps());
            }
        }
    }

    private void closeKey(SelectionKey key) {
        key.cancel();
        IOUtils.closeQuietly(key.channel());
    }

    private void readFrom(SocketChannel channel) {
        logger.debug("Read event.");
        SocketPair pair = pairsRead.get(channel);
        if (pair == null) {
            IOUtils.closeQuietly(channel);
            logger.error("Channel without a pair.");
            return;
        }
        try {
            readToBuffer(pair);
            writeFromBuffer(pair);
        } catch (IOException e) {
            logger.error("IO.", e);
            closePair(pair);
        }
    }

    private void readToBuffer(final SocketPair pair) throws IOException {
        final int read = pair.src.read(pair.buffer);
        if (read < 0) {
            logger.debug("Mark closing.");
            pair.status.markClosing();
            IOUtils.closeQuietly(pair.src);
        } else {
            pair.status.inc();
            logger.debug("Read {} bytes.", read);
        }
    }

    private void writeFromBuffer(final SocketPair pair) throws IOException {
        final ByteBuffer buffer = pair.buffer;
        buffer.flip();
        final int remain = buffer.remaining();
        final int written = pair.dst.write(buffer);
        logger.debug("Written {} bytes of {}.", written, remain);
        if (buffer.hasRemaining()) {
            logger.debug("Slow consumer.");
            pair.src.register(selector, 0);
            buffer.compact();
            logger.debug("Register flusher.");
            pair.dst.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            pairsFlush.put(pair.dst, pair);
        } else if (pair.status.isClosing()) {
            closePair(pair);
        } else {
            buffer.clear();
        }
    }

    private void closePair(final SocketPair pair) {
        logger.debug("Close pair.");
        SocketPair otherPair = pairsRead.remove(pair.dst);
        pairsRead.remove(pair.src);
        pairsFlush.remove(pair.src);
        pairsFlush.remove(pair.dst);
        pair.close();
        if (otherPair != null) {
            otherPair.close();
        } else {
            throw new RuntimeException("Pair is missing.");
        }
    }

    private void flushBuffer(SocketChannel channel) {
        SocketPair pair = pairsFlush.get(channel);
        if (pair == null) {
            IOUtils.closeQuietly(channel);
            logger.error("Channel without a pair.");
            return;
        }
        ByteBuffer buffer = pair.buffer;
        buffer.flip();
        try {
            final int remain = buffer.remaining();
            final int written = pair.dst.write(buffer);
            logger.debug("Flush {} bytes of {}.", written, remain);
            if (buffer.hasRemaining()) {
                buffer.compact();
                return;
            }
            buffer.clear();
            pair.dst.register(selector, SelectionKey.OP_READ);
            pairsFlush.remove(pair.dst);
            if (pair.status.isClosing()) {
                closePair(pair);
            } else {
                pair.src.register(selector, SelectionKey.OP_READ);
            }
        } catch (IOException e) {
            logger.error("Write.", e);
            closePair(pair);
        }
    }

    private void accept(SelectionKey key) {
        final SocketChannel in = incomingChannel((ServerSocketChannel) key.channel());
        if (in == null) {
            return;
        }
        final SocketChannel out = outgoingChannel();
        if (out == null) {
            IOUtils.closeQuietly(in);
            return;
        }
        final Status status = new Status();
        pairsRead.put(in, new SocketPair(ByteBuffer.allocate(bufferSize), in, out, status));
        pairsRead.put(out, new SocketPair(ByteBuffer.allocate(bufferSize), out, in, status));
        logger.info("Forward {}.", target);
    }

    private SocketChannel outgoingChannel() {
        try {
            SocketChannel outChannel = SocketChannel.open();
            try {
                outChannel.configureBlocking(false);
                outChannel.connect(target);
                while (!outChannel.finishConnect()) {
                    logger.debug("Wait finish connect.");
                }
                outChannel.register(selector, SelectionKey.OP_READ);
                return outChannel;
            } catch (ClosedSelectorException | IOException e) {
                IOUtils.closeQuietly(outChannel);
                logger.error("Open socket {}.", target, e);
                return null;
            }
        } catch (IOException e) {
            logger.error("Open socket {}.", target, e);
            return null;
        }
    }

    private SocketChannel incomingChannel(ServerSocketChannel channel) {
        try {
            final SocketChannel inChannel = channel.accept();
            try {
                inChannel.configureBlocking(false);
                inChannel.register(selector, SelectionKey.OP_READ);
                return inChannel;
            } catch (ClosedSelectorException |IOException e) {
                logger.error("Incoming connection from {} failed.",
                        inChannel.getRemoteAddress(), e);
                IOUtils.closeQuietly(inChannel);
                return null;
            }
        } catch (IOException e) {
            logger.error("Incoming connection failed.", e);
            return null;
        }
    }    
}
