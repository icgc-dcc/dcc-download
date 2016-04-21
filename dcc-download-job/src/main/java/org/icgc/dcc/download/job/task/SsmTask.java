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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.function.AddFieldsToKey;
import org.icgc.dcc.download.job.function.ConvertRecord;
import org.icgc.dcc.download.job.function.FilterSsm;
import org.icgc.dcc.download.job.function.PairByFields;
import org.icgc.dcc.download.job.function.UnwindRow;
import org.icgc.dcc.download.job.function.UnwindUnresolvedValue;

import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class SsmTask extends GenericTask {

  @NonNull
  private final Collection<Marking> access;

  @Override
  protected JavaRDD<String> process(JavaRDD<Row> input, DownloadDataType dataType) {

    return input
        // Get values from SSM Occurrence
        .mapToPair(new PairByFields(dataType.getFirstLevelFields()))

        // Unwind observations
        .flatMapValues(new UnwindRow(ImmutableList.of(FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME)))
        .filter(new FilterSsm(access))

        // Add Observation fields to the resolved values
        .mapToPair(new AddFieldsToKey(dataType.getSecondLevelFields()))

        // Unwind Consequences
        .flatMapToPair(new UnwindUnresolvedValue(FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME))

        .map(new ConvertRecord(dataType.getDownloadFileds()));
  }

}
