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
package org.icgc.dcc.download.job.task;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.icgc.dcc.download.core.model.DownloadDataType.SGV_CONTROLLED;
import lombok.val;

import org.icgc.dcc.download.job.core.AbstractSparkJobTest;
import org.icgc.dcc.download.job.core.StaticDownloadJob;
import org.junit.Test;

public class ProjectStaticTaskTest extends AbstractSparkJobTest {

  ProjectStaticTask task = new ProjectStaticTask();

  @Test
  public void testExecute() throws Exception {
    prepareInput();
    val taskContext = createTaskContext();
    task.execute(taskContext);
    // prepareVerificationFiles();
  }

  private TaskContext createTaskContext() {
    return createTaskContext(StaticDownloadJob.STATIC_DIR_PATH, emptySet(), singleton(SGV_CONTROLLED));
  }

}
