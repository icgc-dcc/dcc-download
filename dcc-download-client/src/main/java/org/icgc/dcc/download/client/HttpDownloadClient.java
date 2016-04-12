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

import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.val;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.model.JobProgress;
import org.icgc.dcc.download.core.request.GetJobsInfoRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.DataTypeSizesResponse;
import org.icgc.dcc.download.core.response.JobInfoResponse;
import org.icgc.dcc.download.core.response.JobsProgressResponse;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.api.json.JSONConfiguration;

public class HttpDownloadClient {

  private static final String JOBS_PATH = "/jobs";
  private static final String STATS_PATH = "/stats";

  private WebResource resource;

  public HttpDownloadClient(@NonNull String baseUrl) {
    val jerseyClient = Client.create(getClientConfig());
    // TODO: externalize
    jerseyClient.addFilter(new LoggingFilter());
    this.resource = jerseyClient.resource(baseUrl);
  }

  public String submitJob(@NonNull SubmitJobRequest requestBody) {
    return resource.path(JOBS_PATH)
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(String.class, requestBody);
  }

  public void cancelJob(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId)
        .delete();
  }

  public Map<String, JobProgress> getJobsProgress(@NonNull Set<String> jobIds) {
    val requestBody = new GetJobsInfoRequest(jobIds);
    val response = resource.path(JOBS_PATH).path("progress")
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(JobsProgressResponse.class, requestBody);

    return response.getJobProgress();
  }

  public Map<String, JobInfo> getJobsInfo(@NonNull Set<String> jobIds) {
    val requestBody = new GetJobsInfoRequest(jobIds);
    val response = resource.path(JOBS_PATH).path("info")
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(JobInfoResponse.class, requestBody);

    return response.getInfo();
  }

  public void setActiveDownload(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId).path("active")
        .post();
  }

  public void unsetActiveDownload(@NonNull String jobId) {
    resource.path(JOBS_PATH).path(jobId).path("active")
        .delete();
  }

  public Map<DownloadDataType, Long> getSizes(@NonNull SubmitJobRequest requestBody) {
    val response = resource.path(STATS_PATH)
        .header(CONTENT_TYPE, JSON_UTF_8)
        .post(DataTypeSizesResponse.class, requestBody);

    return response.getSizes();

  }

  private static ClientConfig getClientConfig() {
    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    cc.getClasses().add(JacksonJsonProvider.class);

    return cc;
  }

}
