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

import lombok.val;

import org.icgc.dcc.download.client.impl.HttpClient;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class HttpClientIntegrationTest {

  private static final String BASE_URL = "http://localhost:8080";

  HttpClient client;

  @Before
  public void setUp() {
    client = new HttpClient(BASE_URL);
  }

  @Test
  @Ignore
  public void testGetSizes() throws Exception {
    val requestBody = SubmitJobRequest.builder()
        .donorIds(ImmutableSet.of("DO001"))
        .dataTypes(ImmutableSet.of(DownloadDataType.DONOR))
        .jobInfo(JobInfo.builder().build())
        .userEmailAddress("")
        .build();

    client.getSizes(requestBody);
  }

  @Test
  @Ignore
  public void testSubmitJob() throws Exception {
    val requestBody = SubmitJobRequest.builder()
        .donorIds(ImmutableSet.of("DO001"))
        .dataTypes(ImmutableSet.of(DownloadDataType.DONOR))
        .jobInfo(JobInfo.builder().build())
        .userEmailAddress("")
        .build();
    client.submitJob(requestBody);
  }

}
