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
package org.icgc.dcc.download.core.model;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;

import org.icgc.dcc.common.core.model.Identifiable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Data type requested by the portal users.
 */
@Getter
public enum DownloadDataType implements Identifiable {

  // The order of fields is important as it will be used in the output file.

  DONOR(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      // TODO: Confirm if should be excluded. It's not present in the dictionary
      .put("study_donor_involved_in", "study_donor_involved_in")
      .put("donor_id", "submitted_donor_id")
      .put("donor_sex", "donor_sex")
      .put("donor_vital_status", "donor_vital_status")
      .put("disease_status_last_followup", "disease_status_last_followup")
      .put("donor_relapse_type", "donor_relapse_type")
      .put("donor_age_at_diagnosis", "donor_age_at_diagnosis")
      .put("donor_age_at_enrollment", "donor_age_at_enrollment")
      .put("donor_age_at_last_followup", "donor_age_at_last_followup")
      .put("donor_relapse_interval", "donor_relapse_interval")
      .put("donor_diagnosis_icd10", "donor_diagnosis_icd10")
      .put("donor_tumour_staging_system_at_diagnosis", "donor_tumour_staging_system_at_diagnosis")
      .put("donor_tumour_stage_at_diagnosis", "donor_tumour_stage_at_diagnosis")
      .put("donor_tumour_stage_at_diagnosis_supplemental", "donor_tumour_stage_at_diagnosis_supplemental")
      .put("donor_survival_time", "donor_survival_time")
      .put("donor_interval_of_last_followup", "donor_interval_of_last_followup")
      .put("prior_malignancy", "prior_malignancy")
      .put("cancer_type_prior_malignancy", "cancer_type_prior_malignancy")
      .put("cancer_history_first_degree_relative", "cancer_history_first_degree_relative")
      .build()),

  DONOR_FAMILY(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("donor_id", "submitted_donor_id")
      .put("donor_has_relative_with_cancer_history", "donor_has_relative_with_cancer_history")
      .put("relationship_type", "relationship_type")
      .put("relationship_type_other", "relationship_type_other")
      .put("relationship_sex", "relationship_sex")
      .put("relationship_age", "relationship_age")
      .put("relationship_disease_icd10", "relationship_disease_icd10")
      .put("relationship_disease", "relationship_disease")
      .build()),

  DONOR_THERAPY(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("donor_id", "submitted_donor_id")
      .put("first_therapy_type", "first_therapy_type")
      .put("first_therapy_therapeutic_intent", "first_therapy_therapeutic_intent")
      .put("first_therapy_start_interval", "first_therapy_start_interval")
      .put("first_therapy_duration", "first_therapy_duration")
      .put("first_therapy_response", "first_therapy_response")
      .put("second_therapy_type", "second_therapy_type")
      .put("second_therapy_therapeutic_intent", "second_therapy_therapeutic_intent")
      .put("second_therapy_start_interval", "second_therapy_start_interval")
      .put("second_therapy_duration", "second_therapy_duration")
      .put("second_therapy_response", "second_therapy_response")
      .put("other_therapy", "other_therapy")
      .put("other_therapy_response", "other_therapy_response")
      .build()),

  DONOR_EXPOSURE(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("donor_id", "submitted_donor_id")
      .put("exposure_type", "exposure_type")
      .put("exposure_intensity", "exposure_intensity")
      .put("tobacco_smoking_history_indicator", "tobacco_smoking_history_indicator")
      .put("tobacco_smoking_intensity", "tobacco_smoking_intensity")
      .put("alcohol_history", "alcohol_history")
      .put("alcohol_history_intensity", "alcohol_history_intensity")
      .build()),

  SPECIMEN(ImmutableMap.<String, String> builder()
      .put("_specimen_id", "icgc_specimen_id")
      .put("_project_id", "project_code")
      // TODO: missing in the dictionary
      .put("study_specimen_involved_in", "study_specimen_involved_in")
      .put("specimen_id", "submitted_specimen_id")
      .put("_donor_id", "icgc_donor_id")
      .put("donor_id", "submitted_donor_id")
      .put("specimen_type", "specimen_type")
      .put("specimen_type_other", "specimen_type_other")
      .put("specimen_interval", "specimen_interval")
      .put("specimen_donor_treatment_type", "specimen_donor_treatment_type")
      .put("specimen_donor_treatment_type_other", "specimen_donor_treatment_type_other")
      .put("specimen_processing", "specimen_processing")
      .put("specimen_processing_other", "specimen_processing_other")
      .put("specimen_storage", "specimen_storage")
      .put("specimen_storage_other", "specimen_storage_other")
      .put("tumour_confirmed", "tumour_confirmed")
      .put("specimen_biobank", "specimen_biobank")
      .put("specimen_biobank_id", "specimen_biobank_id")
      .put("specimen_available", "specimen_available")
      .put("tumour_histological_type", "tumour_histological_type")
      .put("tumour_grading_system", "tumour_grading_system")
      .put("tumour_grade", "tumour_grade")
      .put("tumour_grade_supplemental", "tumour_grade_supplemental")
      .put("tumour_stage_system", "tumour_stage_system")
      .put("tumour_stage", "tumour_stage")
      .put("tumour_stage_supplemental", "tumour_stage_supplemental")
      .put("digital_image_of_stained_section", "digital_image_of_stained_section")
      .put("percentage_cellularity", "percentage_cellularity")
      .put("level_of_cellularity", "level_of_cellularity")
      .build()),

  SAMPLE(ImmutableMap.<String, String> builder()
      .put("_sample_id", "icgc_sample_id")
      .put("_project_id", "project_code")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("_specimen_id", "icgc_specimen_id")
      .put("specimen_id", "submitted_specimen_id")
      .put("_donor_id", "icgc_donor_id")
      .put("donor_id", "submitted_donor_id")
      .put("analyzed_sample_interval", "analyzed_sample_interval")
      .put("percentage_cellularity", "percentage_cellularity")
      .put("level_of_cellularity", "level_of_cellularity")
      .put("study", "study")
      .build()),

  CNSM(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("_matched_sample_id", "matched_icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("mutation_type", "mutation_type")
      .put("copy_number", "copy_number")
      .put("segment_mean", "segment_mean")
      .put("segment_median", "segment_median")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("assembly_version", "assembly_version")
      .put("chromosome_start_range", "chromosome_start_range")
      .put("chromosome_end_range", "chromosome_end_range")
      .put("start_probe_id", "start_probe_id")
      .put("end_probe_id", "end_probe_id")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("is_annotated", "is_annotated")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("gene_build_version", "gene_build_version")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  JCN,
  METH_SEQ,
  METH_ARRAY,
  MIRNA_SEQ,
  STSM,
  PEXP,
  EXP_SEQ,
  EXP_ARRAY,
  SSM_OPEN,
  SSM_CONTROLLED,
  SGV_CONTROLLED,

  // For backward compatible only (remove when no longer use these names)
  EXP,
  MIRNA,
  METH;

  public static final Set<DownloadDataType> CLINICAL = ImmutableSet.of(DONOR, DONOR_FAMILY, DONOR_THERAPY,
      DONOR_EXPOSURE, SPECIMEN, SAMPLE);

  /**
   * A mapping between field names of output archives consumed by the portal users and field names produced by the ETL
   * ExportJob.<br>
   * <b>NB:</b> The fields must be ordered in the order of the output file.
   */
  private final Map<String, String> fields;

  private DownloadDataType() {
    this(Collections.emptyMap());
  }

  private DownloadDataType(@NonNull Map<String, String> fields) {
    this.fields = fields;
  }

  @Override
  public String getId() {
    return name().toLowerCase();
  }

  public boolean isControlled() {
    return getId().endsWith("_controlled");
  }

  public List<String> getDownloadFileds() {
    return this.getFields().entrySet().stream()
        .map(e -> e.getKey())
        .collect(toImmutableList());
  }

}
