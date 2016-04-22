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

import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.icgc.dcc.download.job.utils.RecordConverter;

import scala.Tuple2;

public final class ConvertRecord implements
    Function<Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>, String> {

  /**
   * Configuration.
   */
  private final RecordConverter recordConverter;

  public ConvertRecord(@NonNull List<String> fields) {
    this.recordConverter = new RecordConverter(fields);
  }

  @Override
  public String call(Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> tuple) throws Exception {
    val resolvedValues = tuple._1._1;
    val row = tuple._2;

    return recordConverter.convert(resolvedValues, row);
  }

}