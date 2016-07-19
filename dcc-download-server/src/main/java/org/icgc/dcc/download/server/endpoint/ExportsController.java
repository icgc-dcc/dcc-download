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
import static org.icgc.dcc.download.server.utils.Responses.getFileMimeType;
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
import org.icgc.dcc.download.server.service.ExportsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/exports")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsController {

  @NonNull
  private final ExportsService exportsService;

  @RequestMapping(method = GET)
  public MetadataResponse listMetadata(HttpServletRequest request) {
    val requestUrl = request.getRequestURL().toString();
    val baseUrl = requestUrl.replaceFirst("/exports(/)?$", EMPTY_STRING);

    return exportsService.getMetadata(baseUrl);
  }

  // The ':.+' regex is required to keep the file extension in the path
  @RequestMapping(value = "/{exportId:.+}", method = GET)
  public void downloadArchive(
      @PathVariable("exportId") String exportId,
      @NonNull HttpServletResponse response) throws IOException {
    log.info("Streaming export ID '{}'...", exportId);

    val export = resolveExport(exportId);
    val output = response.getOutputStream();
    @Cleanup
    val streamer = exportsService.getExportStreamer(export, output);
    val fileName = streamer.getName();

    response.setContentType(getFileMimeType(fileName));

    streamer.stream();
    log.info("Finished streaming export ID '{}'...", exportId);
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
