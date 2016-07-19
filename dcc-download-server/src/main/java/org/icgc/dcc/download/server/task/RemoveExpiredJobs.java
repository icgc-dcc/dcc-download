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

import static java.time.Instant.ofEpochSecond;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.model.DataFiles;
import org.icgc.dcc.download.server.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("production")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RemoveExpiredJobs {

  private static final String ID_FIELD = "_id";

  @NonNull
  private final MongoTemplate mongoTemplate;

  // Daily at midnight
  @Scheduled(cron = "0 0 0 * * *")
  public void execute() {
    val ids = findAllIds();
    if (ids.isEmpty()) {
      log.info("No download jobs found. Skipping cleanup...");
      return;
    }

    log.info("Data file IDs: {}", ids);
    val expired = findExpired(ids, getExpirationDate());
    if (expired.isEmpty()) {
      log.info("No expired download jobs found. Skipping cleanup...");
      return;
    }

    log.info("Expired data file IDs: {}", expired);
    remove(expired);
  }

  private List<String> findAllIds() {
    val query = new Query();
    query.fields().include(ID_FIELD);
    val dataFiles = mongoTemplate.find(query, DataFiles.class);

    return dataFiles.stream()
        .map(dataFile -> dataFile.getId())
        .collect(toImmutableList());
  }

  private List<String> findExpired(List<String> ids, long expiration) {
    log.info("Looking for jobs older than: {}", ofEpochSecond(expiration));
    val query = query(where(ID_FIELD).in(ids).and("submissionDate").lt(expiration));
    query.fields().include(ID_FIELD);
    val jobs = mongoTemplate.find(query, Job.class);

    return jobs.stream()
        .map(job -> job.getId())
        .collect(toImmutableList());
  }

  private void remove(List<String> ids) {
    val query = query(where(ID_FIELD).in(ids));
    val result = mongoTemplate.remove(query, DataFiles.class);
    log.info("Removed {} jobs", result.getN());
  }

  private long getExpirationDate() {
    return Instant.now().minus(1, ChronoUnit.DAYS).getEpochSecond();
  }

}
