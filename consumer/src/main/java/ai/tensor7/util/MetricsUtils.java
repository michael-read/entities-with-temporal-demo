package ai.tensor7.util;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MetricsUtils {

  /**
   * Starts HttpServer to expose a scrape endpoint. See
   * https://micrometer.io/docs/registry/prometheus for more info.
   */
  // NOTE: Prometheus target config requires: fallback_scrape_protocol: "PrometheusText0.0.4" since Context Type isn't properly recorded
  public static HttpServer startPrometheusScrapeEndpoint(
      PrometheusMeterRegistry registry, int port) {
    try {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", httpExchange -> {
            String response = registry.scrape("text/plain");
            httpExchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = httpExchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        new Thread(server::start).start();
        return server;
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }
}
