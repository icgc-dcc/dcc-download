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
package org.icgc.dcc.download.client;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;

import java.util.Set;

import lombok.NonNull;
import lombok.val;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobStatusResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class HttpDownloadClient {

  private static final String DATA_TYPES_PARAM = "dataTypes";
  private static final String DONOR_IDS_PARAM = "donorIds";
  private static final String JOBS_PATH = "/jobs";

  private WebResource resource;

  public HttpDownloadClient(@NonNull String baseUrl) {
    val jerseyClient = Client.create(getClientConfig());
    this.resource = jerseyClient.resource(baseUrl);
  }

  public String submitJob(@NonNull Set<String> donorIds, @NonNull Set<DownloadDataType> dataTypes) {
    val requestBody = createSubmitJobRequestBody(donorIds, dataTypes);
    return resource.path(JOBS_PATH).path("submit")
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(String.class, requestBody);

  }

  public void cancelJob(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId)
        .delete();
  }

  public JobStatusResponse getJobStatus(@NonNull String jobId) {
    return resource.path(JOBS_PATH).path(jobId).path("status")
        .header(CONTENT_TYPE, JSON_UTF_8)
        .get(JobStatusResponse.class);
  }

  public void setActiveDownload(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId).path("active")
        .post();
  }

  public void unsetActiveDownload(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId).path("active")
        .delete();
  }

  private JsonNode createSubmitJobRequestBody(Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    val request = Jackson.DEFAULT.createObjectNode();
    request.putPOJO(DONOR_IDS_PARAM, donorIds);
    request.putPOJO(DATA_TYPES_PARAM, dataTypes);

    return request;
  }

  private static ClientConfig getClientConfig() {
    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    cc.getClasses().add(JacksonJsonProvider.class);

    return cc;
  }

}
