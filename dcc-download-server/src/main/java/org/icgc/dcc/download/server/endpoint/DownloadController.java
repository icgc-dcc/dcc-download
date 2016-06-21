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
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.download.server.utils.Responses.throwBadRequestException;
import static org.icgc.dcc.download.server.utils.Responses.throwForbiddenException;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.core.jwt.JwtService;
import org.icgc.dcc.download.core.model.TokenPayload;
import org.icgc.dcc.download.core.request.RecordsSizeRequest;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.DataTypeSizesResponse;
import org.icgc.dcc.download.core.response.JobResponse;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.service.ArchiveDownloadService;
import org.icgc.dcc.download.server.utils.Collections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.io.Files;
import com.google.common.net.MediaType;

@Slf4j
@RestController
@RequestMapping("/downloads")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class DownloadController {

  @NonNull
  private final ArchiveDownloadService downloadService;
  @NonNull
  private final JwtService tokenService;

  @RequestMapping(method = POST)
  public String submitJob(@RequestBody SubmitJobRequest request) {
    log.debug("Received submit job request {}", request);
    if (!isValid(request)) {
      log.info("Malformed submission job request. Skipping submission... {}", request);
      throw new BadRequestException("Malformed submission job request");
    }

    return downloadService.submitDownloadRequest(request);
  }

  @RequestMapping(method = GET)
  public void downloadArchive(
      @RequestParam("token") String token,
      @RequestParam(value = "type", required = false) String dataType,
      @NonNull HttpServletResponse response) throws IOException {
    log.debug("Received download request. Token: '{}'. Type: '{}'", token, dataType);
    val tokenPayload = getTokenPayload(token);

    val jobId = tokenPayload.getId();
    val user = tokenPayload.getUser();
    if (!downloadService.isUserDownload(jobId, user)) {
      log.warn("Access forbidden. User: '{}'. Download ID: '{}'", user, jobId);
      throwForbiddenException();
    }

    val output = response.getOutputStream();
    val downloadDataType = getDownloadDataType(dataType);
    val streamerOpt = downloadDataType.isPresent() ?
        downloadService.getArchiveStreamer(jobId, output, downloadDataType.get()) :
        downloadService.getArchiveStreamer(jobId, output);

    streamArchive(Optional.of(jobId), streamerOpt, response);
  }

  @RequestMapping(value = "/static", method = GET)
  public void staticDownload(@RequestParam("token") String token, HttpServletResponse response) throws IOException {
    log.debug("Received download request. Token: '{}'", token);
    val tokenPayload = getTokenPayload(token);
    val requestPath = tokenPayload.getPath();
    log.debug("Static path: '{}'", requestPath);
    val filePath = getFsPath(requestPath);
    log.info("Getting download archive for path '{}'", filePath);

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

  private TokenPayload getTokenPayload(@NonNull String token) {
    TokenPayload payload = null;
    try {
      payload = tokenService.parseToken(token);
    } catch (Exception e) {
      log.warn("Failed to parse token '{}'", token);
      throwBadRequestException("Invalid download token.");
    }

    return payload;
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
    if (jobInfo == null || isNullOrEmpty(jobInfo.getUser())) {
      valid = false;
    }

    return valid;
  }

  private static Optional<DownloadDataType> getDownloadDataType(String dataType) {
    if (isNullOrEmpty(dataType)) {
      return Optional.empty();
    }

    return Optional.of(DownloadDataType.valueOf(dataType.toUpperCase()));
  }

  private static String getFileMimeType(String filename) {
    val extension = Files.getFileExtension(filename);
    switch (extension) {
    case "gz":
      return MediaType.GZIP.toString();
    case "tar":
      return MediaType.TAR.toString();
    case "txt":
      MediaType.PLAIN_TEXT_UTF_8.toString();
      return null;
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

  private static void streamArchive(Optional<String> jobId, Optional<FileStreamer> streamerOpt,
      HttpServletResponse response)
      throws IOException {
    if (jobId.isPresent()) {
      checkJobExistence(jobId.get(), streamerOpt);
    }

    if (!streamerOpt.isPresent()) {
      throw new NotFoundException("The file not found");
    }

    val streamer = streamerOpt.get();
    val filename = streamer.getName();

    response.setContentType(getFileMimeType(filename));
    response.addHeader(CONTENT_DISPOSITION, "attachment; filename=" + filename);

    streamer.stream();
    streamer.close();
  }

}
