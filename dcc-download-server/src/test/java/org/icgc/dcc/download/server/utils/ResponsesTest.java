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
package org.icgc.dcc.download.server.utils;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.download.core.model.JobStatus.SUCCEEDED;
import static org.icgc.dcc.download.server.utils.Responses.createJobResponse;
import lombok.val;

import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ResponsesTest {

  @Test
  public void testCreateJobResponse_empty() throws Exception {
    val actualJob = createJobResponse(createJob(), emptyList());
    val expectedJob = Job.builder().id("1").build();
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  @Test
  public void testCreateJobResponse_some() throws Exception {
    val actualJob = createJobResponse(createJob(), ImmutableList.of("submissionDate", "ttlHours"));
    val expectedJob = Job.builder()
        .id("1")
        .submissionDate(1L)
        .ttlHours(1)
        .build();
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  private static Job createJob() {
    return Job.builder()
        .id("1")
        .donorIds(singleton("DO1"))
        .dataTypes(singleton(DONOR))
        .status(SUCCEEDED)
        .submissionDate(1L)
        .completionDate(1L)
        .jobInfo(JobUiInfo.builder().build())
        .fileSizeBytes(1L)
        .ttlHours(1)
        .progress(emptyMap())
        .build();
  }

}
