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

import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.io.OutputStream;
import java.util.Optional;

import lombok.val;

import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.download.core.jwt.JwtService;
import org.icgc.dcc.download.core.jwt.exception.InvalidJwtTokenException;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.model.TokenPayload;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.service.ArchiveDownloadService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(MockitoJUnitRunner.class)
public class DownloadControllerTest {

  private static final String ENDPOINT_PATH = "/downloads";

  @Mock
  ArchiveDownloadService downloadService;
  @Mock
  JwtService tokenService;
  @Mock
  FileStreamer streamer;

  @InjectMocks
  DownloadController controller;

  MockMvc mockMvc;

  @Before
  public void setUp() {
    mockMvc = standaloneSetup(controller).build();
  }

  @Test
  public void testSubmitJob() throws Exception {
    val request = SubmitJobRequest.builder()
        .donorIds(singleton("DO1"))
        .dataTypes(singleton(DONOR))
        .jobInfo(JobUiInfo.builder()
            .user("user")
            .build())
        .submissionTime(123L)
        .build();

    val requestBody = Jackson.DEFAULT.writeValueAsString(request);

    mockMvc
        .perform(post(ENDPOINT_PATH)
            .contentType(APPLICATION_JSON)
            .content(requestBody))
        .andExpect(status().isOk());
  }

  @Test
  public void testSubmitJob_noDonors() throws Exception {
    val request = SubmitJobRequest.builder()
        .dataTypes(singleton(DONOR))
        .jobInfo(JobUiInfo.builder()
            .user("user")
            .build())
        .submissionTime(123L)
        .build();

    val requestBody = Jackson.DEFAULT.writeValueAsString(request);

    mockMvc
        .perform(post(ENDPOINT_PATH)
            .contentType(APPLICATION_JSON)
            .content(requestBody))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testSubmitJob_noDataTypes() throws Exception {
    val request = SubmitJobRequest.builder()
        .donorIds(singleton("DO1"))
        .jobInfo(JobUiInfo.builder()
            .user("user")
            .build())
        .submissionTime(123L)
        .build();

    val requestBody = Jackson.DEFAULT.writeValueAsString(request);

    mockMvc
        .perform(post(ENDPOINT_PATH)
            .contentType(APPLICATION_JSON)
            .content(requestBody))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testSubmitJob_noUser() throws Exception {
    val request = SubmitJobRequest.builder()
        .donorIds(singleton("DO1"))
        .dataTypes(singleton(DONOR))
        .jobInfo(JobUiInfo.builder().build())
        .submissionTime(123L)
        .build();

    val requestBody = Jackson.DEFAULT.writeValueAsString(request);

    mockMvc
        .perform(post(ENDPOINT_PATH)
            .contentType(APPLICATION_JSON)
            .content(requestBody))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testDownloadArchive() throws Exception {
    when(tokenService.parseToken("zzz123")).thenReturn(new TokenPayload("id1", "user1", null));
    when(downloadService.isUserDownload("id1", "user1")).thenReturn(true);
    when(downloadService.getArchiveStreamer(eq("id1"), any(OutputStream.class))).thenReturn(Optional.of(streamer));
    when(streamer.getName()).thenReturn("1.tar");

    mockMvc
        .perform(get(ENDPOINT_PATH).param("token", "zzz123"))
        .andExpect(status().isOk());
  }

  @Test
  public void testDownloadArchive_invalidToken() throws Exception {
    when(tokenService.parseToken("zzz123")).thenThrow(new InvalidJwtTokenException());

    mockMvc
        .perform(get(ENDPOINT_PATH).param("token", "zzz123"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testDownloadArchive_permissionDenied() throws Exception {
    when(tokenService.parseToken("zzz123")).thenReturn(new TokenPayload("id1", "user1", null));
    when(downloadService.isUserDownload("id1", "user1")).thenReturn(false);

    mockMvc
        .perform(get(ENDPOINT_PATH).param("token", "zzz123"))
        .andExpect(status().isForbidden());
  }

}
