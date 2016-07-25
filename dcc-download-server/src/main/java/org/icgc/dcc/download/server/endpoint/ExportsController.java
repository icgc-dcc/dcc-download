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
import static org.icgc.dcc.download.server.model.Export.RELEASE_CONTROLLED;
import static org.icgc.dcc.download.server.utils.Responses.getFileMimeType;
import static org.icgc.dcc.download.server.utils.Responses.throwBadRequestException;
import static org.icgc.dcc.download.server.utils.Responses.throwForbiddenException;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.model.Export;
import org.icgc.dcc.download.server.model.MetadataResponse;
import org.icgc.dcc.download.server.service.AuthService;
import org.icgc.dcc.download.server.service.ExportsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/exports")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsController {

  @NonNull
  private final ExportsService exportsService;
  @NonNull
  private final AuthService tokenService;

  @RequestMapping(method = GET)
  public MetadataResponse listMetadata(
      @RequestHeader(value = "Authorization", required = false) String authHeader,
      HttpServletRequest request) {
    val requestUrl = request.getRequestURL().toString();
    val baseUrl = requestUrl.replaceFirst("/exports(/)?$", EMPTY_STRING);

    MetadataResponse metadata;
    if (isAuthorized(authHeader)) {
      log.info("Serving controlled exports metadata...");
      metadata = exportsService.getControlledMetadata(baseUrl);
    } else {
      log.info("Serving open exports metadata...");
      metadata = exportsService.getMetadata(baseUrl);
    }

    return metadata;
  }

  // The ':.+' regex is required to keep the file extension in the path
  @RequestMapping(value = "/{exportId:.+}", method = GET)
  public void downloadArchive(
      @PathVariable("exportId") String exportId,
      @RequestHeader(value = "Authorization", required = false) String authHeader,
      @NonNull HttpServletResponse response) throws IOException {
    log.info("Received get export archive request for id '{}'", exportId);
    val export = resolveExport(exportId);
    val output = response.getOutputStream();
    if (export == RELEASE_CONTROLLED && !isAuthorized(authHeader)) {
      log.warn("Client requested controlled archive without authorization. Authorization header: '{}'", authHeader);
      throwForbiddenException();
    }

    @Cleanup
    val streamer = exportsService.getExportStreamer(export, output);
    val fileName = streamer.getName();

    response.setContentType(getFileMimeType(fileName));

    log.info("Streaming export ID '{}'...", exportId);
    streamer.stream();
    log.info("Finished streaming export ID '{}'...", exportId);
  }

  private boolean isAuthorized(String authHeader) {
    if (isNullOrEmpty(authHeader)) {
      return false;
    }

    try {
      val token = tokenService.parseToken(authHeader);

      return tokenService.isAuthorized(token);
    } catch (IllegalArgumentException e) {
      throwBadRequestException("Invalid authorization header", e);
    }

    // Won't come to this point
    return false;
  }

  private static Export resolveExport(String exportId) {
    try {
      return Export.fromId(exportId);
    } catch (IllegalArgumentException e) {
      log.warn("Couldn't find export entity with ID '{}'", exportId);
      throw new NotFoundException(format("%s not found", exportId), e);
    }
  }

}
