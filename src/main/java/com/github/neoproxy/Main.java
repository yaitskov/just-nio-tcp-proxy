package com.github.neoproxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Daneel Yaitskov
 */
public class Main {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Map<Integer, TcpProxy> proxies = new HashMap<>();
    private int counter;
    private int bufferSize = 64 * 1024;

    public static void main(String[] args) throws IOException {
        Main main = new Main();
        try {
            main.shell();
        } finally {
            main.close();
        }
    }

    public void close() {
        for (TcpProxy proxy : proxies.values()) {
            proxy.close();
        }
        executor.shutdown();
    }

    public void shell() throws IOException {
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            out("$ ");
            if (!dispatch(in)) {
                break;
            }
        }
    }

    private void out(String message) {
        System.out.print(message);
    }

    private boolean dispatch(BufferedReader in) throws IOException {
        String line = in.readLine();
        if (line == null) {
            return false;
        }
        String[] cmd = line.split(" ");
        if (cmd.length == 0) {
            return true;
        }
        switch (cmd[0]) {
            case "start":
                start(cmd);
                break;
            case "stop":
                stop(cmd);
                break;
            case "size":
                size(cmd);
                break;
            case "exit":
                return false;
            default:
                out("Unknown command\n");
                break;
        }
        return true;
    }

    private void size(String[] cmd) {
        if (cmd.length != 2) {
            out("Bad arguments.\n");
            return;
        }
        try {
            bufferSize = Math.max(1, Integer.parseInt(cmd[1]));
        } catch (NumberFormatException e) {
            out("Bad arguments.\n");
        }
    }

    private void start(String[] cmd) {
        if (cmd.length != 4) {
            out("Bad arguments.\n");
            return;
        }
        try {
            int localPort = Integer.parseInt(cmd[1]);
            String host = cmd[2];
            int targetPort = Integer.parseInt(cmd[3]);
            TcpProxy proxy = new TcpProxy(localPort, TimeUnit.SECONDS.toMillis(10),
                    new InetSocketAddress(host, targetPort), bufferSize);
            proxies.put(++counter, proxy);
            executor.submit(proxy);
            out("Proxy id " + counter + "\n");
        } catch (NumberFormatException e) {
            out("Bad arguments.\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void stop(String[] cmd) {
        for (int i = 1; i < cmd.length; ++i) {
            try {
                int id = Integer.parseInt(cmd[i]);
                TcpProxy proxy = proxies.remove(id);
                if (proxy == null) {
                    out("Proxy " + id + " not found.\n");
                    return;
                }
                proxy.close();
            } catch (NumberFormatException e) {
                out("Bad arguments.\n");
            }
        }
    }
}
