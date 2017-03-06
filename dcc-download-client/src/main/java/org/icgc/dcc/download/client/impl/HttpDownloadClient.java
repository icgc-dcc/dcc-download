/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.download.client.impl;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.GZIP;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static org.icgc.dcc.common.core.model.DownloadDataType.CLINICAL;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.download.core.util.Endpoints.DOWNLOADS_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.HEALTH_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.LIST_FILES_PATH;

import java.net.ConnectException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.security.DumbX509TrustManager;
import org.icgc.dcc.download.client.DownloadClient;
import org.icgc.dcc.download.client.DownloadClientConfig;
import org.icgc.dcc.download.client.response.HealthResponse;
import org.icgc.dcc.download.core.DownloadServiceUnavailableException;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.request.RecordsSizeRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.DataTypeSizesResponse;
import org.icgc.dcc.download.core.response.JobResponse;

import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpDownloadClient implements DownloadClient {

  /**
   * Dependencies.
   */
  private final WebResource resource;

  public HttpDownloadClient(@NonNull DownloadClientConfig config) {
    val jerseyClient = configureHttpClient(config);
    this.resource = jerseyClient.resource(config.baseUrl());
  }

  @Override
  public boolean isServiceAvailable() {
    HealthResponse response;
    try {
      response = resource.path(HEALTH_PATH)
          .header(CONTENT_TYPE, JSON_UTF_8)
          .get(HealthResponse.class);
    } catch (ClientHandlerException e) {
      log.error("Exception during the health check:\n", e);

      return false;
    }

    return response.isAlive();
  }

  @Override
  public String submitJob(
      @NonNull Set<String> donorIds,
      @NonNull Set<DownloadDataType> dataTypes,
      @NonNull JobUiInfo jobInfo) {
    val submitJobRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(resolveSubmitDataTypes(dataTypes))
        .jobInfo(jobInfo)
        .submissionTime(Instant.now().getEpochSecond())
        .build();

    try {
      val response = resource.path(DOWNLOADS_PATH)
          .header(CONTENT_TYPE, JSON_UTF_8)
          .post(ClientResponse.class, submitJobRequest);

      if (!isSuccessful(response)) {
        return null;
      }

      return response.getEntity(String.class);
    } catch (ClientHandlerException e) {
      return rethrowConnectionRefused(e);
    }
  }

  private static boolean isSuccessful(ClientResponse response) {
    return response.getStatus() == 200;
  }

  @Override
  public JobResponse getJob(@NonNull String jobId) {
    try {
      val request = resource.path(DOWNLOADS_PATH).path(jobId).path("info");
      val response = request.get(ClientResponse.class);

      if (!isSuccessful(response)) {
        return null;
      }

      return response.getEntity(JobResponse.class);
    } catch (ClientHandlerException e) {
      return rethrowConnectionRefused(e);
    }
  }

  @Override
  public Map<DownloadDataType, Long> getSizes(@NonNull Set<String> donorIds) {
    try {
      val body = new RecordsSizeRequest(donorIds);
      val response = resource.path(DOWNLOADS_PATH).path("size")
          .header(CONTENT_TYPE, JSON_UTF_8)
          .header(CONTENT_ENCODING, GZIP)
          .post(ClientResponse.class, body);

      if (!isSuccessful(response)) {
        return null;
      }

      return response.getEntity(DataTypeSizesResponse.class).getSizes();
    } catch (ClientHandlerException e) {
      return rethrowConnectionRefused(e);
    }
  }

  @Override
  public Collection<DownloadFile> listFiles(String path) {
    return listFiles(path, false);
  }

  @Override
  public Collection<DownloadFile> listFiles(String path, boolean recursive) {
    try {
      val response = resource.path(LIST_FILES_PATH).path(path)
          .queryParam("recursive", Boolean.toString(recursive))
          .get(ClientResponse.class);

      if (!isSuccessful(response)) {
        return null;
      }

      return response.getEntity(new GenericType<Collection<DownloadFile>>() {});
    } catch (ClientHandlerException e) {
      return rethrowConnectionRefused(e);
    }
  }

  private static Set<DownloadDataType> resolveSubmitDataTypes(Set<DownloadDataType> dataTypes) {
    return dataTypes.contains(DONOR) ? ImmutableSet.<DownloadDataType> builder()
        .addAll(dataTypes)
        .addAll(CLINICAL)
        .build() : dataTypes;
  }

  // HttpClient configuration

  private static Client configureHttpClient(DownloadClientConfig config) {
    ClientConfig clientConfig = getClientConfig();

    // Configure SSL
    clientConfig = config.strictSSLCertificates() ? configureSSLCertificatesHandling(clientConfig) : clientConfig;

    val jerseyClient = Client.create(clientConfig);
    // Configure auth
    if (isAuthEnabled(config)) {
      jerseyClient.addFilter(new HTTPBasicAuthFilter(config.user(), config.password()));
    }

    // Configure logging
    if (config.requestLoggingEnabled()) {
      jerseyClient.addFilter(new LoggingFilter());
    }

    return jerseyClient;
  }

  private static ClientConfig getClientConfig() {
    ClientConfig config = new DefaultClientConfig();
    config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    config.getClasses().add(JacksonJsonProvider.class);
    config.getProperties()
        .put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName());

    return config;
  }

  @SneakyThrows
  private static ClientConfig configureSSLCertificatesHandling(ClientConfig config) {
    val context = SSLContext.getInstance("TLS");
    context.init(null, new TrustManager[] { new DumbX509TrustManager() }, null);

    config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(
        new HostnameVerifier() {

          @Override
          public boolean verify(String hostname, SSLSession sslSession) {
            return true;
          }

        }, context));

    return config;
  }

  private static boolean isAuthEnabled(DownloadClientConfig config) {
    return !isNullOrEmpty(config.user());
  }

  private static <T> T rethrowConnectionRefused(ClientHandlerException exception) {
    val cause = exception.getCause();
    if (cause instanceof ConnectException && "Connection refused".equals(cause.getMessage())) {
      return throwServiceUnavailableException();
    } else {
      throw exception;
    }
  }

  private static <T> T throwServiceUnavailableException() {
    throw new DownloadServiceUnavailableException();
  }

}
