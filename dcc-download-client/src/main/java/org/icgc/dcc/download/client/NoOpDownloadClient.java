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
package org.icgc.dcc.download.client;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.model.JobProgress;

public class NoOpDownloadClient implements DownloadClient {

  @Override
  public boolean isServiceAvailable() {
    return false;
  }

  @Override
  public void cancelJob(String jobId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, JobInfo> getJobsInfo(Set<String> jobIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, JobProgress> getJobsProgress(Set<String> jobIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<DownloadDataType, Long> getSizes(Set<String> donorIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setActiveDownload(String jobId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean streamArchiveInGz(OutputStream out, String downloadId, DownloadDataType dataType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean streamArchiveInTarGz(OutputStream out, String downloadId, List<DownloadDataType> downloadDataTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String submitJob(Set<String> donorIds, Set<DownloadDataType> dataTypes, JobInfo jobInfo,
      String userEmailAddress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsetActiveDownload(String jobId) {
    throw new UnsupportedOperationException();
  }

}
