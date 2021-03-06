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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.common.core.model.DownloadDataType.SPECIMEN;
import static org.icgc.dcc.common.core.model.DownloadDataType.SSM_CONTROLLED;

import java.util.Collections;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.client.DownloadClientConfig;
import org.icgc.dcc.download.client.util.AbstractHttpTest;
import org.icgc.dcc.download.core.DownloadServiceUnavailableException;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class HttpDownloadClientTest extends AbstractHttpTest {

  private static final String JOB_ID = "job123";

  HttpDownloadClient downloadClient;
  HttpDownloadClient connectionRefusedDownloadClient;

  @Before
  public void setUp() {
    val config = new DownloadClientConfig().baseUrl(getServerUrl()).requestLoggingEnabled(true);
    downloadClient = new HttpDownloadClient(config);
    config.baseUrl("http://localhost:55555");
    connectionRefusedDownloadClient = new HttpDownloadClient(config);
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
  public void testSubmitJob_badRequest() throws Exception {
    // Setup
    stubGetSizesRequest();
    stubFor(post(urlEqualTo("/downloads"))
        .willReturn(aResponse()
            .withStatus(400)
        ));

    // Run
    val jobId = downloadClient.submitJob(
        ImmutableSet.of("DO1"),
        ImmutableSet.of(DONOR, SSM_CONTROLLED),
        JobUiInfo.builder().build());

    // Verify
    assertThat(jobId).isNull();
  }

  @Test
  public void testGetSizes() throws Exception {
    stubGetSizesRequest();

    val sizes = downloadClient.getSizes(Collections.singleton("DO1"));
    log.info("{}", sizes);
    assertThat(sizes).isEqualTo(ImmutableMap.of(DONOR, 1L, SSM_CONTROLLED, 2L, SAMPLE, 0L, SPECIMEN, 1L));
  }

  @Test
  public void testGetSizes_badRequest() throws Exception {
    stubFor(post(urlEqualTo("/downloads/size")).withRequestBody(equalToJson("{\"donorIds\":[\"DO1\"]}"))
        .willReturn(aResponse().withStatus(400)
        ));

    val sizes = downloadClient.getSizes(Collections.emptySet());
    assertThat(sizes).isNull();
  }

  private static void stubSubmitJobRequest() {
    stubFor(post(urlEqualTo("/downloads"))
        // Stubbing of body fails because of the submissionTime

        // .withRequestBody(equalToJson("{\"dataTypes\":[\"DONOR\",\"SSM_CONTROLLED\",\"SPECIMEN\"],"
        // + "\"donorIds\":[\"DO1\"],"
        // + "\"jobInfo\":{\"email\":null,\"uiQueryStr\":null,\"controlled\":false,\"filter\":null},"
        // + "\"submissionTime\":0}"))
        .willReturn(aResponse()
            .withBody(JOB_ID)
        ));
  }

  private static void stubGetSizesRequest() {
    stubFor(post(urlEqualTo("/downloads/size")).withRequestBody(equalToJson("{\"donorIds\":[\"DO1\"]}"))
        .willReturn(aResponse()
            .withBody("{\"sizes\":{\"DONOR\":1,\"SSM_CONTROLLED\":2,\"SAMPLE\":0,\"SPECIMEN\":1}}")
            .withHeader(CONTENT_TYPE, JSON_UTF_8.toString())
        ));
  }

  @Test
  public void testGetJob_notFound() throws Exception {
    stubFor(get(urlEqualTo(format("/downloads/%s/info", JOB_ID)))
        .willReturn(aResponse()
            .withStatus(404)
        ));
    val info = downloadClient.getJob(JOB_ID);
    assertThat(info).isNull();
  }

  @Test
  public void testIsServiceAvailable() throws Exception {
    val available = connectionRefusedDownloadClient.isServiceAvailable();
    assertThat(available).isFalse();
  }

  @Test(expected = DownloadServiceUnavailableException.class)
  public void failedSubmitJobTest() {
    connectionRefusedDownloadClient.submitJob(
        ImmutableSet.of("DO1"),
        ImmutableSet.of(DONOR, SSM_CONTROLLED),
        JobUiInfo.builder().build());
  }

  @Test(expected = DownloadServiceUnavailableException.class)
  public void failedGetJobTest() {
    connectionRefusedDownloadClient.getJob("");
  }

  @Test(expected = DownloadServiceUnavailableException.class)
  public void failedGetSizesTest() {
    connectionRefusedDownloadClient.getSizes(Collections.singleton("DO1"));
  }

  @Test(expected = DownloadServiceUnavailableException.class)
  public void failedListFilesTest() {
    connectionRefusedDownloadClient.listFiles("");
  }

}
