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
package org.icgc.dcc.download.server.service;

import static org.icgc.dcc.download.server.utils.Jobs.completeJob;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CompleteJobUpdater implements Runnable {

  /**
   * Dependencies.
   */
  @Autowired
  private CompletionService<String> completionService;
  @Autowired
  private JobRepository repository;
  @Resource(name = "submittedJobs")
  private Map<String, Future<String>> submittedJobs;

  @PostConstruct
  public void startDaemon() {
    val thread = new Thread(this);
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  @SneakyThrows
  public void run() {
    while (true) {
      val result = completionService.take();
      try {
        val jobId = result.get();
        log.info("Job '{}' is completed.", jobId);

        log.debug("Looking for job to update...");
        val job = repository.findById(jobId);
        log.debug("Updating {} ...", job);

        val completedJob = completeJob(job);
        repository.save(completedJob);
        log.debug("Update job {}", completedJob);

        submittedJobs.remove(jobId);
        // TODO: send email
      } catch (CancellationException e) {
        log.debug("Job was cancelled: ", e);
      } catch (ExecutionException e) {
        log.error("Encountered an error while executing job.\n", e);
      } catch (InterruptedException e) {
        log.info("Job was cancelled: ", e);
      }
    }
  }

}
