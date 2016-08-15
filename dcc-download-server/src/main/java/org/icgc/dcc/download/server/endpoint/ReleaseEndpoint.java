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
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.web.bind.annotation.RequestMethod.PUT;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.fs.DownloadFileSystem;
import org.icgc.dcc.download.server.fs.DownloadFilesReader;
import org.icgc.dcc.download.server.fs.ReleaseView;
import org.icgc.dcc.download.server.fs.RootView;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@ConfigurationProperties(prefix = "endpoints." + ReleaseEndpoint.ENDPOINT_ID, ignoreUnknownFields = false)
public class ReleaseEndpoint implements MvcEndpoint {

  /**
   * Constants.
   */
  protected static final String ENDPOINT_ID = "release";

  @NonNull
  private final FileSystemService fileSystemService;
  @NonNull
  private final DownloadFileSystem downloadFileSystem;
  @NonNull
  private final RootView rootView;
  @NonNull
  private final ReleaseView releaseView;
  @NonNull
  private final DownloadFilesReader downloadFilesReader;

  @RequestMapping(value = "/{releaseName}", method = PUT)
  public ResponseEntity<String> addRelease(@PathVariable("releaseName") String releaseName) {
    if (!downloadFilesReader.isValidReleaseName(releaseName)) {
      log.warn("Failed to verify release '{}' against the release name regex", releaseName);

      return getNotFoundResponse("Invalid release '%s'", releaseName);
    }

    try {
      log.info("Loading release '{}'...", releaseName);
      fileSystemService.loadRelease(releaseName, downloadFilesReader);
      downloadFileSystem.setReleases(fileSystemService.getReleases());
      val currentRelease = fileSystemService.getCurrentRelease();
      rootView.setCurrentRelease(currentRelease);
      releaseView.setCurrentRelease(currentRelease);
      log.info("Loaded release '{}'", releaseName);
    } catch (NotFoundException e) {
      val message = format("Not found release '%s'", releaseName);
      log.warn(message);

      return getNotFoundResponse(message);
    }

    return ResponseEntity.ok().body("Updated");
  }

  @Override
  public String getPath() {
    return "/" + ENDPOINT_ID;
  }

  @Override
  public boolean isSensitive() {
    return true;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Class<? extends Endpoint> getEndpointType() {
    return null;
  }

  private static ResponseEntity<String> getNotFoundResponse(String message, Object... args) {
    val body = format(message, args);

    return ResponseEntity.status(NOT_FOUND).body(body);
  }

}
