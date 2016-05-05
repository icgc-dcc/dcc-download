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
package org.icgc.dcc.download.server.repository;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import lombok.val;

import org.icgc.dcc.download.server.model.Job;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestMongoConfig.class)
public class JobRepositoryTest {

  public static final String DB_NAME = "dcc-download-test";

  @Autowired
  JobRepository repository;
  @Autowired
  MongoTemplate mongoTemplate;
  @Autowired
  Mongo mongo;

  @After
  public void tearDown() {
    mongo.dropDatabase(DB_NAME);
  }

  @Test
  public void testFindByCompletionDateLessThan() throws Exception {
    // Setup
    mongoTemplate.save(createJob("1", now().minus(47, HOURS)));
    mongoTemplate.save(createJob("2", now().minus(49, HOURS)));
    mongoTemplate.save(createJob("3", now()));

    val expirationDate = now().minus(48, HOURS);
    val allJobs = repository.findByCompletionDateLessThan(expirationDate.toEpochMilli());

    assertThat(allJobs).hasSize(1);
    val expired = allJobs.get(0);
    assertThat(expired.getId()).isEqualTo("2");
  }

  private static Job createJob(String id, Instant completionDate) {
    return Job.builder()
        .id(id)
        .completionDate(completionDate.toEpochMilli())
        .build();
  }

}