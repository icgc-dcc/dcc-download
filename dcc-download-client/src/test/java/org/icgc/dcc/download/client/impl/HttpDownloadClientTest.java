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
package org.icgc.dcc.download.client.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.download.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.download.core.model.DownloadDataType.SPECIMEN;
import static org.icgc.dcc.download.core.model.DownloadDataType.SSM_CONTROLLED;

import java.util.Collections;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.client.DownloadClientConfig;
import org.icgc.dcc.download.client.fs.ArchiveOutputStream;
import org.icgc.dcc.download.client.util.AbstractHttpTest;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class HttpDownloadClientTest extends AbstractHttpTest {

  private static final String JOB_ID = "job123";

  @Mock
  ArchiveOutputStream outputStream;

  HttpDownloadClient downloadClient;

  @Before
  public void setUp() {
    val config = new DownloadClientConfig().baseUrl(getServerUrl()).requestLoggingEnabled(true);
    downloadClient = new HttpDownloadClient(outputStream, config);
  }

  @Test
  public void testSubmitClinicalJob() throws Exception {
    // Setup
    stubGetSizesRequest();
    stubSubmitJobRequest();

    // Run
    val jobId = downloadClient.submitJob(
        ImmutableSet.of("DO1"),
        ImmutableSet.of(DONOR, SSM_CONTROLLED),
        JobUiInfo.builder().build());

    // Verify
    assertThat(jobId).isEqualTo(JOB_ID);
  }

  @Test
  public void testGetSizes() throws Exception {
    stubGetSizesRequest();

    val sizes = downloadClient.getSizes(Collections.singleton("DO1"));
    log.info("{}", sizes);
    assertThat(sizes).isEqualTo(ImmutableMap.of(DONOR, 1L, SSM_CONTROLLED, 2L, SAMPLE, 0L, SPECIMEN, 1L));
  }

  private static void stubSubmitJobRequest() {
    stubFor(post(urlEqualTo("/jobs"))
        .withRequestBody(equalToJson("{\"dataTypes\":[\"DONOR\",\"SSM_CONTROLLED\",\"SPECIMEN\"],"
            + "\"donorIds\":[\"DO1\"],"
            + "\"jobInfo\":{\"email\":null,\"uiQueryStr\":null,\"controlled\":false,\"filter\":null},"
            + "\"submissionTime\":0}"))
        .willReturn(aResponse()
            .withBody(JOB_ID)
        ));
  }

  private static void stubGetSizesRequest() {
    stubFor(post(urlEqualTo("/stats")).withRequestBody(equalToJson("{\"donorIds\":[\"DO1\"]}"))
        .willReturn(aResponse()
            .withBody("{\"sizes\":{\"DONOR\":1,\"SSM_CONTROLLED\":2,\"SAMPLE\":0,\"SPECIMEN\":1}}")
            .withHeader(CONTENT_TYPE, JSON_UTF_8.toString())
        ));
  }

}
