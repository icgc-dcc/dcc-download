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

import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.core.request.RecordsSizeRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.DataTypeSizesResponse;
import org.icgc.dcc.download.core.response.JobResponse;
import org.icgc.dcc.download.server.io.ArchiveStreamer;
import org.icgc.dcc.download.server.service.ArchiveDownloadService;
import org.icgc.dcc.download.server.utils.Collections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.HandlerMapping;

import com.google.common.io.Files;
import com.google.common.net.MediaType;

@Slf4j
@RestController
@RequestMapping("/downloads")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class DownloadController {

  @NonNull
  private final ArchiveDownloadService downloadService;

  @RequestMapping(method = POST)
  public String submitJob(@RequestBody SubmitJobRequest request) {
    log.debug("Received submit job request {}", request);
    if (!isValid(request)) {
      log.info("Malformed submission job request. Skipping submission... {}", request);
      throw new BadRequestException("Malformed submission job request");
    }

    return downloadService.submitDownloadRequest(request);
  }

  @RequestMapping(value = "/{jobId:.+}", method = GET)
  public void download(@PathVariable("jobId") String jobId, HttpServletResponse response) throws IOException {
    val output = response.getOutputStream();
    val streamerOpt = downloadService.getArchiveStreamer(jobId, output);
    streamArchive(Optional.of(jobId), streamerOpt, response);
  }

  @RequestMapping(value = "/{jobId:.+}/{dataType}", method = GET)
  public void downloadDataType(
      @PathVariable("jobId") String jobId,
      @PathVariable("dataType") String dataType,
      HttpServletResponse response) throws IOException {
    val downloadDataType = DownloadDataType.valueOf(dataType.toUpperCase());
    val output = response.getOutputStream();
    val streamerOpt = downloadService.getArchiveStreamer(jobId, output, downloadDataType);
    streamArchive(Optional.of(jobId), streamerOpt, response);
  }

  @RequestMapping(value = "/static/**", method = GET)
  public void staticDownload(HttpServletRequest request, HttpServletResponse response) throws IOException {
    val requestPath = (String) request.getAttribute(
        HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
    val filePath = getFsPath(requestPath);

    val output = response.getOutputStream();
    val streamerOpt = downloadService.getStaticArchiveStreamer(filePath, output);
    streamArchive(Optional.empty(), streamerOpt, response);
  }

  @RequestMapping(value = "/{jobId:.+}/info", method = GET)
  public JobResponse getArchiveInfo(@PathVariable("jobId") String jobId) {
    val infoOpt = downloadService.getArchiveInfo(jobId);
    checkJobExistence(jobId, infoOpt);

    return infoOpt.get();
  }

  @RequestMapping(value = "/size", method = POST)
  public DataTypeSizesResponse getSizes(@RequestBody RecordsSizeRequest request) {
    val filesSize = downloadService.getFilesSize(request.getDonorIds());

    return new DataTypeSizesResponse(filesSize);
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
    if (jobInfo == null) {
      valid = false;
    }

    return valid;
  }

  private static String getFileMimeType(String filename) {
    val extension = Files.getFileExtension(filename);
    switch (extension) {
    case "gz":
      return MediaType.GZIP.toString();
    case "tar":
      return MediaType.TAR.toString();
    default:
      log.error("Failed to resolve Mime-Type from file name '{}'", filename);
      throw new BadRequestException("Invalid request");
    }
  }

  private static void checkJobExistence(String jobId, Optional<?> optional) {
    if (!optional.isPresent()) {
      throw new NotFoundException(format("Job '%s' was not found.", jobId));
    }
  }

  private static String getFsPath(String requestUrl) {
    val fsPath = requestUrl
        .replaceFirst("/downloads", EMPTY_STRING)
        .replaceFirst("/static", EMPTY_STRING)
        .replaceFirst("/$", EMPTY_STRING);

    return fsPath.isEmpty() ? "/" : fsPath;
  }

  private static void streamArchive(Optional<String> jobId, Optional<ArchiveStreamer> streamerOpt,
      HttpServletResponse response)
      throws IOException {
    if (jobId.isPresent()) {
      checkJobExistence(jobId.get(), streamerOpt);
    }

    val streamer = streamerOpt.get();
    val filename = streamer.getName();

    response.setContentType(getFileMimeType(filename));
    response.addHeader(CONTENT_DISPOSITION, "attachment; filename=" + filename);

    streamer.stream();
    streamer.close();
  }

}
