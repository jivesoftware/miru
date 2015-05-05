/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.metric.sampler;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class HttpPoster implements MiruMetricSampleSender {

    private static final String path = "/miru/sea/anomaly/intake";
    private final Gson gson = new Gson();
    private final String host;
    private final int port;
    private final long soTimeout;
    private Socket socket;
    private BufferedWriter wr;
    private BufferedReader rd;

    public HttpPoster(String host, int port, long soTimeout) throws IOException {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
    }

    void connect() throws UnsupportedEncodingException, IOException {
        socket = new Socket(host, port);
        socket.setKeepAlive(true);
        socket.setSoTimeout((int) soTimeout);
        wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF8"));
        rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    void post(String data) throws IOException {
        wr.write("POST " + path + " HTTP/1.0\r\n");
        wr.write("Content-Length: " + data.length() + "\r\n");
        wr.write("Content-Type: application/json\r\n");
        wr.write("\r\n");
        wr.write(data);
        wr.flush();
        BufferedReader rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            //System.out.println(line);
        }
    }

    void destroy() {
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
    public void send(List<AnomalyMetric> events) throws Exception {
        if (socket == null) {
            connect();
        }
        String toJson = gson.toJson(events);
        try {
            post(toJson);
        } catch (SocketException x) {
            destroy();
            throw x;
        } catch (Exception x) {
            System.err.println("Failed to metrics sizeInBytes:" + toJson.length() + " to http://" + host + ":" + port + "" + path);
            x.printStackTrace();
            destroy();
            throw x;
        }
    }

}
