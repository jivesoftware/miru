package com.jivesoftware.os.miru.logappender;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
class HttpPoster implements MiruLogSender {

    private static final String path = "/miru/stumptown/intake";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String host;
    private final int port;
    private final long soTimeout;

    private Socket socket;
    private BufferedWriter wr;
    private BufferedReader rd;

    HttpPoster(String host, int port, long soTimeout) throws IOException {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
    }

    private void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setKeepAlive(true);
        socket.setSoTimeout((int) soTimeout);

        wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    private void post(String data) throws IOException {
        wr.write("POST " + path + " HTTP/1.1\r\n");
        wr.write("Content-Length: " + data.length() + "\r\n");
        wr.write("Content-Type: application/json\r\n");
        wr.write("\r\n");
        wr.write(data);
        wr.flush();
        BufferedReader rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            System.out.println(line);
        }
    }

    private void destroy() {
        try {
            wr.close();
        } catch (Exception x) {
        }
        try {
            rd.close();
        } catch (Exception x) {
        }
        socket = null;
        wr = null;
        rd = null;
    }

    @Override
    public void send(List<MiruLogEvent> events) throws Exception {
        if (socket == null) {
            connect();
        }

        String toJson = objectMapper.writeValueAsString(events);
        try {
            post(toJson);
        } catch (SocketException x) {
            destroy();
            throw x;
        } catch (Exception x) {
            System.err.println("Failed to log append sizeInBytes:" + toJson.length() + " to http://" + host + ":" + port + "" + path + " ");
            x.printStackTrace();
            destroy();
            throw x;
        }
    }

}
