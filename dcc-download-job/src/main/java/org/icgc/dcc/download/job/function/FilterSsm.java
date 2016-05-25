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
package org.icgc.dcc.download.job.function;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.download.job.utils.Rows;

import scala.Tuple2;

@RequiredArgsConstructor
public final class FilterSsm implements
    Function<Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>, Boolean> {

  @NonNull
  private final Collection<Marking> access;

  @Override
  public Boolean call(Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> tuple) throws Exception {
    val row = tuple._2;
    val markingValue = Rows.getValue(row, FieldNames.NormalizerFieldNames.NORMALIZER_MARKING);

    val resolvedMarking = Marking.from(markingValue);
    checkState(resolvedMarking.isPresent(), "Failed to resolve marking from value '%s'", markingValue);

    return access.contains(resolvedMarking.get());
  }

}