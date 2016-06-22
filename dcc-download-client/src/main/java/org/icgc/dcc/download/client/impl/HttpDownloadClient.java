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
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.System.currentTimeMillis;
import static org.icgc.dcc.common.core.model.DownloadDataType.CLINICAL;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.download.core.util.Endpoints.DOWNLOADS_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.HEALTH_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.LIST_FILES_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.STATIC_DOWNLOADS_PATH;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.security.DumbX509TrustManager;
import org.icgc.dcc.download.client.DownloadClient;
import org.icgc.dcc.download.client.DownloadClientConfig;
import org.icgc.dcc.download.client.response.HealthResponse;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.request.RecordsSizeRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.DataTypeSizesResponse;
import org.icgc.dcc.download.core.response.JobResponse;

import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

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
    } catch (Exception e) {
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
    val submitDataTypes = resolveDataTypes(donorIds, dataTypes);
    val submitJobRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(submitDataTypes)
        .jobInfo(jobInfo)
        .submissionTime(currentTimeMillis())
        .build();

    return resource.path(DOWNLOADS_PATH)
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(String.class, submitJobRequest);
  }

  @Override
  public JobResponse getJob(@NonNull String jobId) {
    val request = resource.path(DOWNLOADS_PATH).path(jobId).path("info");

    return request.get(JobResponse.class);
  }

  @Override
  public void setActiveDownload(@NonNull String jobId) {
    resource.path(DOWNLOADS_PATH).path(jobId).path("active")
        .put();
  }

  @Override
  public void unsetActiveDownload(@NonNull String jobId) {
    resource.path(DOWNLOADS_PATH).path(jobId).path("active")
        .delete();
  }

  @Override
  public Map<DownloadDataType, Long> getSizes(@NonNull Set<String> donorIds) {
    val body = new RecordsSizeRequest(donorIds);
    val response = resource.path(DOWNLOADS_PATH).path("size")
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(DataTypeSizesResponse.class, body);

    return response.getSizes();
  }

  @Override
  public Collection<DownloadFile> listFiles(String path) {
    return resource.path(LIST_FILES_PATH).path(path)
        .get(new GenericType<Collection<DownloadFile>>() {});
  }

  @Override
  public String getReadme(@NonNull String token) {
    val response = resource.path(STATIC_DOWNLOADS_PATH)
        .queryParam("token", token)
        .get(String.class);

    return response;
  }

  private Set<DownloadDataType> resolveDataTypes(Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    val submitDataTypes = resolveSubmitDataTypes(dataTypes);
    val sizes = getSizes(donorIds);

    return submitDataTypes.stream()
        .filter(dt -> sizes.get(dt) != null && sizes.get(dt) > 0L)
        .collect(toImmutableSet());
  }

  private static Set<DownloadDataType> resolveSubmitDataTypes(Set<DownloadDataType> dataTypes) {
    return dataTypes.contains(DONOR) ?
        ImmutableSet.<DownloadDataType> builder()
            .addAll(dataTypes)
            .addAll(CLINICAL)
            .build() :
        dataTypes;
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
    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    cc.getClasses().add(JacksonJsonProvider.class);

    return cc;
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

        }, context
        ));

    return config;
  }

  private static boolean isAuthEnabled(DownloadClientConfig config) {
    return !isNullOrEmpty(config.user());
  }

}
