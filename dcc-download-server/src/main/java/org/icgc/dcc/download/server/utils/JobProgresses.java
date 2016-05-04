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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;

import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobProgress;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.model.TaskProgress;

@NoArgsConstructor(access = PRIVATE)
public final class JobProgresses {

  public static JobProgress createJobProgress(@NonNull JobStatus status, @NonNull Set<DownloadDataType> dataTypes) {
    return status == JobStatus.CANCELLED ?
        new JobProgress(status, createTaskProgress(dataTypes, 0)) :
        new JobProgress(status, createTaskProgress(dataTypes, 1));
  }

  private static Map<DownloadDataType, TaskProgress> createTaskProgress(Set<DownloadDataType> dataTypes, int progress) {
    return dataTypes.stream()
        .collect(toImmutableMap(dt -> dt, dt -> new TaskProgress(progress, progress)));
  }

}
