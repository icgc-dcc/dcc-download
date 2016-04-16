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
package org.icgc.dcc.download.job.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

@Slf4j
public class DefaultDownloadJobTest extends AbstractSparkJobTest {

  private static final String JOB_ID = "job123";

  DefaultDownloadJob job = new DefaultDownloadJob();

  @Test
  public void testExecute() throws Exception {
    prepareInput();
    val jobContext = createJobContext();
    job.execute(jobContext);
    val outputPath = workingDir.getAbsolutePath() + "/" + JOB_ID + "/" + DownloadDataType.DONOR.getId()
        + "/part-00000.gz";
    log.debug("Expected output file: {}", outputPath);
    val outputFile = new File(outputPath);
    log.info("\nResult: {}", new Object[] { new File(workingDir, JOB_ID).list() });
    assertThat(outputFile.exists()).isTrue();
  }

  private JobContext createJobContext() {
    return createJobContext(JOB_ID, ImmutableSet.of("DO001", "DO002"), DownloadDataType.CLINICAL);
  }

}
