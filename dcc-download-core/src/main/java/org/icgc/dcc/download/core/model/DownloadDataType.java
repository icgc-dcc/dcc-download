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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.core.model.DownloadDataTypeFields.SSM_CONTROLLED_FIELDS;
import static org.icgc.dcc.download.core.model.DownloadDataTypeFields.SSM_CONTROLLED_REMOVE_FIELDS;
import static org.icgc.dcc.download.core.model.DownloadDataTypeFields.SSM_FIRST_LEVEL_FIELDS;
import static org.icgc.dcc.download.core.model.DownloadDataTypeFields.SSM_SECOND_LEVEL_FIELDS;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Identifiable;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.common.core.util.stream.Collectors;
import org.icgc.dcc.common.core.util.stream.Streams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

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
      .build(),

      ImmutableList.<String> builder()
          .add("_donor_id")
          .add("_project_id")
          .add("_specimen_id")
          .add("_sample_id")
          .add("_matched_sample_id")
          .add("analyzed_sample_id")
          .add("matched_sample_id")
          .add("mutation_type")
          .add("copy_number")
          .add("segment_mean")
          .add("segment_median")
          .add("chromosome")
          .add("chromosome_start")
          .add("chromosome_end")
          .add("assembly_version")
          .add("chromosome_start_range")
          .add("chromosome_end_range")
          .add("start_probe_id")
          .add("end_probe_id")
          .add("sequencing_strategy")
          .add("quality_score")
          .add("probability")
          .add("is_annotated")
          .add("verification_status")
          .add("verification_platform")
          .add("platform")
          .add("experimental_protocol")
          .add("base_calling_algorithm")
          .add("alignment_algorithm")
          .add("variation_calling_algorithm")
          .add("other_analysis_algorithm")
          .add("seq_coverage")
          .add("raw_data_repository")
          .add("raw_data_accession")
          .add("consequence") // Unwind field
          .build()),

  JCN(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("junction_id", "junction_id")
      .put("gene_stable_id", "gene_stable_id")
      .put("gene_chromosome", "gene_chromosome")
      .put("gene_strand", "gene_strand")
      .put("gene_start", "gene_start")
      .put("gene_end", "gene_end")
      .put("assembly_version", "assembly_version")
      .put("second_gene_stable_id", "second_gene_stable_id")
      .put("exon1_chromosome", "exon1_chromosome")
      .put("exon1_number_bases", "exon1_number_bases")
      .put("exon1_end", "exon1_end")
      .put("exon1_strand", "exon1_strand")
      .put("exon2_chromosome", "exon2_chromosome")
      .put("exon2_number_bases", "exon2_number_bases")
      .put("exon2_start", "exon2_start")
      .put("exon2_strand", "exon2_strand")
      .put("is_fusion_gene", "is_fusion_gene")
      .put("is_novel_splice_form", "is_novel_splice_form")
      .put("junction_seq", "junction_seq")
      .put("junction_type", "junction_type")
      .put("junction_read_count", "junction_read_count")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("gene_build_version", "gene_build_version")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  METH_SEQ(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("mirna_db", "mirna_db")
      .put("mirna_id", "mirna_id")
      .put("normalized_read_count", "normalized_read_count")
      .put("raw_read_count", "raw_read_count")
      .put("fold_change", "fold_change")
      .put("is_isomir", "is_isomir")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("sequencing_platform", "sequencing_platform")
      .put("total_read_count", "total_read_count")
      .put("experimental_protocol", "experimental_protocol")
      .put("reference_sample_type", "reference_sample_type")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  METH_ARRAY(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("analysis_id", "analysis_id")
      .put("array_platform", "array_platform")
      .put("probe_id", "probe_id")
      .put("methylation_value", "methylation_value")
      .put("metric_used", "metric_used")
      .put("methylated_probe_intensity", "methylated_probe_intensity")
      .put("unmethylated_probe_intensity", "unmethylated_probe_intensity")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("fraction_wg_cpg_sites_covered", "fraction_wg_cpg_sites_covered")
      .put("conversion_rate", "conversion_rate")
      .put("experimental_protocol", "experimental_protocol")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  MIRNA_SEQ(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("mirna_db", "mirna_db")
      .put("mirna_id", "mirna_id")
      .put("normalized_read_count", "normalized_read_count")
      .put("raw_read_count", "raw_read_count")
      .put("fold_change", "fold_change")
      .put("is_isomir", "is_isomir")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("sequencing_platform", "sequencing_platform")
      .put("total_read_count", "total_read_count")
      .put("experimental_protocol", "experimental_protocol")
      .put("reference_sample_type", "reference_sample_type")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  STSM(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("variant_type ", "variant_type")
      .put("sv_id", "sv_id")
      .put("placement", "placement")
      .put("annotation", "annotation")
      .put("interpreted_annotation", "interpreted_annotation")
      .put("chr_from", "chr_from")
      .put("chr_from_bkpt", "chr_from_bkpt")
      .put("chr_from_strand", "chr_from_strand")
      .put("chr_from_range", "chr_from_range")
      .put("chr_from_flanking_seq", "chr_from_flanking_seq")
      .put("chr_to", "chr_to")
      .put("chr_to_bkpt", "chr_to_bkpt")
      .put("chr_to_strand", "chr_to_strand")
      .put("chr_to_range", "chr_to_range")
      .put("chr_to_flanking_seq", "chr_to_flanking_seq")
      .put("assembly_version", "assembly_version")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("microhomology_sequence", "microhomology_sequence")
      .put("non_templated_sequence", "non_templated_sequence")
      .put("evidence", "evidence")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("zygosity", "zygosity")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("gene_affected_by_bkpt_from", "gene_affected_by_bkpt_from")
      .put("gene_affected_by_bkpt_to", "gene_affected_by_bkpt_to")
      .put("transcript_affected_by_bkpt_from", "transcript_affected_by_bkpt_from")
      .put("transcript_affected_by_bkpt_to", "transcript_affected_by_bkpt_to")
      .put("bkpt_from_context", "bkpt_from_context")
      .put("bkpt_to_context", "bkpt_to_context")
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
      .build(),

      ImmutableList.<String> builder()
          .add("_donor_id")
          .add("_project_id")
          .add("_specimen_id")
          .add("_sample_id")
          .add("analyzed_sample_id")
          .add("matched_sample_id")
          .add("variant_type ")
          .add("sv_id")
          .add("placement")
          .add("annotation")
          .add("interpreted_annotation")
          .add("chr_from")
          .add("chr_from_bkpt")
          .add("chr_from_strand")
          .add("chr_from_range")
          .add("chr_from_flanking_seq")
          .add("chr_to")
          .add("chr_to_bkpt")
          .add("chr_to_strand")
          .add("chr_to_range")
          .add("chr_to_flanking_seq")
          .add("assembly_version")
          .add("sequencing_strategy")
          .add("microhomology_sequence")
          .add("non_templated_sequence")
          .add("evidence")
          .add("quality_score")
          .add("probability")
          .add("zygosity")
          .add("verification_status")
          .add("verification_platform")
          .add("consequence")
          .add("platform")
          .add("experimental_protocol")
          .add("base_calling_algorithm")
          .add("alignment_algorithm")
          .add("variation_calling_algorithm")
          .add("other_analysis_algorithm")
          .add("seq_coverage")
          .add("raw_data_repository")
          .add("raw_data_accession")
          .add("consequence") // Unwind field
          .build()),

  PEXP(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("antibody_id", "antibody_id")
      .put("gene_name", "gene_name")
      .put("gene_stable_id", "gene_stable_id")
      .put("gene_build_version", "gene_build_version")
      .put("normalized_expression_level", "normalized_expression_level")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build()),

  EXP_SEQ(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("analysis_id", "analysis_id")
      .put("gene_model", "gene_model")
      .put("gene_id", "gene_id")
      .put("normalized_read_count", "normalized_read_count")
      .put("raw_read_count", "raw_read_count")
      .put("fold_change", "fold_change")
      .put("assembly_version", "assembly_version")
      .put("platform", "platform")
      .put("total_read_count", "total_read_count")
      .put("experimental_protocol", "experimental_protocol")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("reference_sample_type", "reference_sample_type")
      .build()),

  EXP_ARRAY(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("analysis_id", "analysis_id")
      .put("gene_model", "gene_model")
      .put("gene_id", "gene_id")
      .put("normalized_expression_value", "normalized_expression_value")
      .put("fold_change", "fold_change")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("reference_sample_type", "reference_sample_type")
      .build()),

  SSM_OPEN(getSsmOpenFields(), SSM_FIRST_LEVEL_FIELDS, getSsmOpenSecondLevelFields()),
  SSM_CONTROLLED(SSM_CONTROLLED_FIELDS, SSM_FIRST_LEVEL_FIELDS, SSM_SECOND_LEVEL_FIELDS),
  SGV_CONTROLLED(ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("analysis_id", "analysis_id")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("variant_type", "variant_type")
      .put("reference_genome_allele", "reference_genome_allele")
      .put("genotype", "genotype")
      .put("variant_allele", "variant_allele")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("total_read_count", "total_read_count")
      .put("variant_allele_read_count", "variant_allele_read_count")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("consequence_type", "consequence_type")
      .put("aa_change", "aa_change")
      .put("cds_change", "cds_change")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("note", "note")
      .build(),

      ImmutableList.<String> builder()
          .add("_donor_id")
          .add("_project_id")
          .add("_specimen_id")
          .add("_sample_id")
          .add("analyzed_sample_id")
          .add("analysis_id")
          .add("chromosome")
          .add("chromosome_start")
          .add("chromosome_end")
          .add("chromosome_strand")
          .add("assembly_version")
          .add("variant_type")
          .add("reference_genome_allele")
          .add("genotype")
          .add("variant_allele")
          .add("quality_score")
          .add("probability")
          .add("total_read_count")
          .add("variant_allele_read_count")
          .add("verification_status")
          .add("verification_platform")
          .add("platform")
          .add("experimental_protocol")
          .add("base_calling_algorithm")
          .add("alignment_algorithm")
          .add("variation_calling_algorithm")
          .add("other_analysis_algorithm")
          .add("sequencing_strategy")
          .add("seq_coverage")
          .add("raw_data_repository")
          .add("raw_data_accession")
          .add("note")
          .add("consequence") // Unwind field
          .build());

  private static final String CONTROLLED_SUFFIX = "_controlled";
  private static final String OPEN_SUFFIX = "_open";

  public static final Set<DownloadDataType> CLINICAL = ImmutableSet.of(DONOR, DONOR_FAMILY, DONOR_THERAPY,
      DONOR_EXPOSURE, SPECIMEN, SAMPLE);

  /**
   * A mapping between field names of output archives consumed by the portal users and field names produced by the ETL
   * ExportJob.<br>
   * <b>NB:</b> The fields must be ordered in the order of the output file.
   */
  private final Map<String, String> fields;

  /**
   * Rows in the parquet files have a nested structure. The processing logic 'unwinds' nested fields to make the row
   * flat. Field levels represent which fields should be used first for projection.<br>
   * E.g. {@code firstLevelFields} are non-nested fields. {@code secondLevelFields} are first nested fields
   */
  private List<String> firstLevelFields;
  private List<String> secondLevelFields;

  private DownloadDataType() {
    this(Collections.emptyMap());
  }

  private DownloadDataType(@NonNull Map<String, String> fields) {
    this(fields, Collections.emptyList(), Collections.emptyList());
  }

  private DownloadDataType(@NonNull Map<String, String> fields, List<String> firstLevelFields) {
    this(fields, firstLevelFields, Collections.emptyList());
  }

  private DownloadDataType(@NonNull Map<String, String> fields, List<String> firstLevelFields,
      List<String> secondLevelFields) {
    this.fields = fields;
    this.firstLevelFields = firstLevelFields;
    this.secondLevelFields = secondLevelFields;
  }

  @Override
  public String getId() {
    return name().toLowerCase();
  }

  public String getCanonicalName() {
    String name = getId();
    if (isControlled()) {
      name = name.replace(CONTROLLED_SUFFIX, Separators.EMPTY_STRING);
    } else if (isOpen()) {
      name = name.replace(OPEN_SUFFIX, Separators.EMPTY_STRING);
    }

    return name;
  }

  public boolean isControlled() {
    return getId().endsWith(CONTROLLED_SUFFIX);
  }

  public boolean isOpen() {
    return getId().endsWith(OPEN_SUFFIX);
  }

  public List<String> getDownloadFileds() {
    return this.getFields().entrySet().stream()
        .map(e -> e.getKey())
        .collect(toImmutableList());
  }

  public static boolean hasClinicalDataTypes(Set<DownloadDataType> dataTypes) {
    return Sets.intersection(CLINICAL, dataTypes)
        .isEmpty() == false;
  }

  public static DownloadDataType from(String name, boolean controlled) {
    val dataTypes = Streams.stream(values())
        .filter(dt -> dt.getCanonicalName().equals(name) && dt.isControlled() == controlled)
        .collect(toImmutableList());
    checkState(dataTypes.size() == 1, "Failed to resolve DownloadDataType from name '%s' and controlled '%s'. "
        + "Found data types: %s", name, controlled, dataTypes);

    return dataTypes.get(0);
  }

  private static Map<String, String> getSsmOpenFields() {
    return SSM_CONTROLLED_FIELDS.entrySet().stream()
        .filter(e -> !SSM_CONTROLLED_REMOVE_FIELDS.contains(e.getKey()))
        .collect(Collectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  private static List<String> getSsmOpenSecondLevelFields() {
    return SSM_SECOND_LEVEL_FIELDS.stream()
        .filter(e -> !SSM_CONTROLLED_REMOVE_FIELDS.contains(e))
        .collect(toImmutableList());
  }

}
