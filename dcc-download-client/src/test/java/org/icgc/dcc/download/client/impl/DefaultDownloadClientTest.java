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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_EXPOSURE;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_FAMILY;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_THERAPY;
import static org.icgc.dcc.download.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.download.core.model.DownloadDataType.SPECIMEN;
import static org.icgc.dcc.download.core.model.DownloadDataType.SSM_CONTROLLED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lombok.val;

import org.icgc.dcc.download.client.impl.DefaultDownloadClient;
import org.icgc.dcc.download.client.io.ArchiveOutputStream;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class DefaultDownloadClientTest {

  @Mock
  ArchiveOutputStream outputStream;
  @Mock
  HttpDownloadClient httpClient;

  DefaultDownloadClient downloadClient;

  @Before
  public void setUp() {
    downloadClient = new DefaultDownloadClient(outputStream, httpClient);
  }

  @Test
  public void testSubmitClinicalJob() throws Exception {
    // Setup
    when(httpClient.getSizes(any(SubmitJobRequest.class)))
        .thenReturn(ImmutableMap.<DownloadDataType, Long> builder()
            .put(DONOR, 1L)
            .put(SPECIMEN, 1L)
            .put(SAMPLE, 0L)
            .put(DONOR_EXPOSURE, 1L)
            .put(DONOR_FAMILY, 0L)
            .put(DONOR_THERAPY, 0L)
            .put(SSM_CONTROLLED, 1L)
            .build());

    // Run
    downloadClient.submitJob(
        ImmutableSet.of("DO1"),
        ImmutableSet.of(DONOR, SSM_CONTROLLED),
        JobInfo.builder().build(),
        "test@example.com");

    // Verify
    val argument = ArgumentCaptor.forClass(SubmitJobRequest.class);
    verify(httpClient).submitJob(argument.capture());

    val submitRequest = argument.getValue();
    assertThat(submitRequest.getDataTypes()).containsOnly(DONOR, SSM_CONTROLLED, SPECIMEN, DONOR_EXPOSURE);
  }

}
