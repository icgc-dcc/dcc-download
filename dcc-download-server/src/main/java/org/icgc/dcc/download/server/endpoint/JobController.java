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

import static org.springframework.http.HttpStatus.OK;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.request.GetJobsInfoRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.JobInfoResponse;
import org.icgc.dcc.download.core.response.JobsProgressResponse;
import org.icgc.dcc.download.server.service.DownloadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public final class JobController {

  @NonNull
  private final DownloadService downloadService;

  @RequestMapping(method = POST)
  public String submitJob(@RequestBody SubmitJobRequest request) {
    log.debug("Received submit job request {}", request);
    if (isEmpty(request)) {
      log.info("Empty submission job request. Skipping submission... {}", request);
      throw new BadRequestException("Empty submission job request");
    }

    val jobId = downloadService.submitJob(request);

    return jobId;
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}", method = DELETE)
  public void cancelJob(@PathVariable("jobId") String jobId) {
    downloadService.cancelJob(jobId);
  }

  @RequestMapping(value = "/progress", method = POST)
  public JobsProgressResponse getJobStatus(@RequestBody GetJobsInfoRequest request) {
    val response = downloadService.getJobsStatus(request.getJobIds());

    return new JobsProgressResponse(response);
  }

  @RequestMapping(value = "/info", method = POST)
  public JobInfoResponse getJobsinfo(@RequestBody GetJobsInfoRequest request) {
    val info = downloadService.getJobsInfo(request.getJobIds());

    return new JobInfoResponse(info);
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}/active", method = POST)
  public void setActiveDownload(@PathVariable("jobId") String jobId) {
    downloadService.setActiveDownload(jobId);
  }

  @ResponseStatus(OK)
  @RequestMapping(value = "/{jobId:.+}/active", method = DELETE)
  public void unsetActiveDownload(@PathVariable("jobId") String jobId) {
    downloadService.unsetActiveDownload(jobId);
  }

  private static boolean isEmpty(SubmitJobRequest request) {
    return request.getDonorIds().isEmpty() || request.getDataTypes().isEmpty();
  }

}
