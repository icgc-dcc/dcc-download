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
package org.icgc.dcc.download.server.endpoint;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.copyOf;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.icgc.dcc.download.core.model.DownloadDataType.values;
import static org.icgc.dcc.download.server.utils.Requests.splitValues;
import static org.icgc.dcc.download.server.utils.Responses.createJobResponse;
import static org.icgc.dcc.download.server.utils.Responses.verifyJobExistance;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.springframework.web.bind.annotation.RequestMethod.PUT;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.server.mail.Mailer;
import org.icgc.dcc.download.server.service.DownloadService;
import org.icgc.dcc.download.server.utils.Collections;
import org.icgc.dcc.download.server.utils.Emails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public final class JobController {

  private static final String PROGRESS_FIELD = "progress";

  @NonNull
  private final DownloadService downloadService;
  @NonNull
  private final Mailer mailer;
  @Value("${mail.serviceUrl}")
  private String serviceEmail;

  @RequestMapping(method = POST)
  public String submitJob(@RequestBody SubmitJobRequest request) {
    log.debug("Received submit job request {}", request);
    if (!isValid(request)) {
      log.info("Malformed submission job request. Skipping submission... {}", request);
      throw new BadRequestException("Malformed submission job request");
    }

    val jobId = downloadService.submitJob(request);
    mailer.sendStart(jobId, request.getJobInfo().getEmail());

    return jobId;
  }

  @RequestMapping(value = "/static", method = POST)
  public String submitStaticJob() {
    log.info("Generating static files...");
    val request = createSubmitStaticJobRequest();

    val jobId = downloadService.submitJob(request);
    mailer.sendStart(jobId, request.getJobInfo().getEmail());

    return jobId;
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}", method = DELETE)
  public void cancelJob(@PathVariable("jobId") String jobId) {
    downloadService.cancelJob(jobId);
  }

  @RequestMapping(value = "/{jobId:.+}", method = GET)
  public Job getJob(@PathVariable("jobId") String jobId, @RequestParam(required = false) String field) {
    log.debug("getJob request: job ID - '{}', field - '{}''", jobId, field);
    val fields = resolveFields(field);
    val job = downloadService.getJob(jobId, includeProgress(fields));
    verifyJobExistance(job, jobId);

    val response = createJobResponse(job, fields);
    log.debug("getJob response: {}", response);

    return response;
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}/active", method = PUT)
  public void setActiveDownload(@PathVariable("jobId") String jobId) {
    downloadService.setActiveDownload(jobId);
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}/active", method = DELETE)
  public void unsetActiveDownload(@PathVariable("jobId") String jobId) {
    downloadService.unsetActiveDownload(jobId);
  }

  private static boolean isValid(SubmitJobRequest request) {
    boolean valid = true;
    val donorIds = request.getDonorIds();
    val dataTypes = request.getDataTypes();
    if (Collections.isNullOrEmpty(donorIds)
        || Collections.isNullOrEmpty(dataTypes)
        || request.getSubmissionTime() == 0) {
      valid = false;
    }

    val jobInfo = request.getJobInfo();
    if (jobInfo == null || isNullOrEmpty(jobInfo.getEmail()) || !Emails.isValidEmail(jobInfo.getEmail())) {
      valid = false;
    }

    return valid;
  }

  private static List<String> resolveFields(String field) {
    return isNullOrEmpty(field) ? emptyList() : splitValues(field);
  }

  private static boolean includeProgress(List<String> fields) {
    return fields.contains(PROGRESS_FIELD);
  }

  private SubmitJobRequest createSubmitStaticJobRequest() {
    return SubmitJobRequest.builder()
        .donorIds(emptySet())
        .dataTypes(copyOf(values()))
        .jobInfo(JobUiInfo.builder()
            .email(serviceEmail)
            .build())
        .submissionTime(currentTimeMillis())
        .build();
  }

}
