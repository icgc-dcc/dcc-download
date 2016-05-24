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
package org.icgc.dcc.download.server.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.JobStatus.EXPIRED;
import static org.icgc.dcc.download.core.model.JobStatus.SUCCEEDED;
import static org.icgc.dcc.download.core.model.JobStatus.TRANSFERRING;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class RemoveExpiredJobsTest {

  private static final String OUTPUT_DIR = "/";

  @Mock
  FileSystem fileSystem;

  @Mock
  JobRepository repository;

  RemoveExpiredJobs task;

  @Before
  public void setUp() {
    val jobProperties = new JobProperties();
    jobProperties.setOutputDir(OUTPUT_DIR);
    task = new RemoveExpiredJobs(fileSystem, repository, jobProperties);
  }

  @Test
  public void testExecute() throws Exception {
    when(repository.findByCompletionDateLessThanAndStatusNot(anyLong(), eq(EXPIRED)))
        .thenReturn(defineJobs());
    val jobPath = new Path(OUTPUT_DIR + "1");
    when(fileSystem.exists(jobPath)).thenReturn(true);

    task.execute();

    verify(fileSystem, times(1)).delete(jobPath, true);
  }

  @Test
  public void testGetExpiredJobs() throws Exception {
    when(repository.findByCompletionDateLessThanAndStatusNot(anyLong(), eq(EXPIRED)))
        .thenReturn(defineJobs());

    val expiredJobs = task.getExpiredJobs();
    assertThat(expiredJobs).hasSize(1);
    assertThat(expiredJobs.get(0).getId()).isEqualTo("1");
  }

  private static List<Job> defineJobs() {
    return ImmutableList.of(
        Job.builder().id("1").status(SUCCEEDED).build(),
        Job.builder().id("2").status(TRANSFERRING).build());
  }

}
