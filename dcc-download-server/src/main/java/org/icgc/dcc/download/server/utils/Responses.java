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

import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Sets.difference;
import static java.util.Arrays.stream;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.server.endpoint.BadRequestException;
import org.icgc.dcc.download.server.endpoint.NotFoundException;

import com.google.common.collect.ImmutableSet;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Responses {

  public static void verifyJobExistance(Job job, String jobId) {
    if (job == null) {
      throwJobNotFoundException(jobId);
    }
  }

  public static void throwJobNotFoundException(String jobId) {
    throw new NotFoundException("Failed to find job with ID " + jobId);
  }

  public static void throwBadRequestException(String message) {
    throw new BadRequestException(message);
  }

  public static Job createJobResponse(Job job, Iterable<String> fieldsProjection) {
    val allFields = getJobFields();
    verifyFields(fieldsProjection, allFields);
    val keepFields = convertProjectionFields(fieldsProjection);
    val removeFields = difference(allFields, keepFields);

    return unsetFields(job, removeFields);
  }

  private static void verifyFields(Iterable<String> fieldsProjection, Set<String> allFields) {
    val valid = Streams.stream(fieldsProjection)
        .allMatch(field -> allFields.contains(field));
    if (!valid) {
      val message = "The request has an invalid field in the field parameter. Fields: " + fieldsProjection;
      log.error(message);
      throwBadRequestException(message);
    }
  }

  private static Job unsetFields(Job job, Set<String> removeFields) {
    removeFields.stream()
        .forEach(field -> unsetField(job, field));

    return job;
  }

  @SneakyThrows
  private static void unsetField(Job job, String field) {
    val f = Job.class.getDeclaredField(field);
    f.setAccessible(true);
    f.set(job, null);
  }

  private static Set<String> convertProjectionFields(Iterable<String> fieldsProjection) {
    val fields = ImmutableSet.<String> builder();
    fields.addAll(fieldsProjection);
    // The 'id' field should never be removed.
    if (!contains(fieldsProjection, "id")) {
      fields.add("id");
    }

    return fields.build();
  }

  private static Set<String> getJobFields() {
    return stream(Job.class.getDeclaredFields())
        .map(field -> field.getName())
        .collect(toImmutableSet());
  }

}
