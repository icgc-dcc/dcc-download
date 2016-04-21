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

import java.util.Collection;

import lombok.val;

import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.core.AbstractSparkJobTest;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class SsmTaskTest extends AbstractSparkJobTest {

  SsmTask task;

  @Test
  public void testExecute_open() throws Exception {
    testExecute(ImmutableSet.of(Marking.OPEN, Marking.MASKED), DownloadDataType.SSM_OPEN);
  }

  @Test
  public void testExecute_controlled() throws Exception {
    testExecute(ImmutableSet.of(Marking.OPEN, Marking.CONTROLLED), DownloadDataType.SSM_CONTROLLED);
  }

  private void testExecute(Collection<Marking> access, DownloadDataType dataType) {
    task = new SsmTask(access);
    prepareInput();
    val taskContext = createTaskContext(dataType);
    task.execute(taskContext);

    prepareVerificationFiles();
    verifyDataTypeOutput(dataType);
  }

}
