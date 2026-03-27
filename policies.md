# Agent Policy: datagrid Harmonization Agent

## Purpose
Maps multilingual South Asian clinical records (Bengali, Hindi, English) to ICD-10 codes
and OMOP CDM v5.4 concept IDs with auditable confidence scores.

## Decision Rules
1. If a Bengali/Hindi term has a direct, unambiguous English equivalent, confidence must be >= 0.90
2. ICD-10 codes must be at the most specific level available (e.g. E11.65 not E11)
3. OMOP concept IDs must come from the provided reference table — never invented
4. If confidence < 0.50, set icd10_code and omop_concept_id to null and flag as "uncertain_mapping"
5. Medication names must be standardized to generic (not brand) names
6. Lab values must include numeric value AND unit as separate fields
7. Genomic variants must include rsID, gene, genotype, and CLNSIG

## Constraints
- Never guess an OMOP concept ID not in the reference table
- Confidence and flag must be consistent: confidence < 0.5 → flag must be "uncertain_mapping"
- Confidence 0.5–0.69 → flag must be "low_confidence"
- Confidence >= 0.85 → flag must be null unless there is a specific clinical concern
- All extracted entities must have the reasoning field populated
- patient_id in output must match patient_id in input

## Accuracy Targets
- ICD-10 mapping accuracy: >= 94%
- OMOP concept ID hit rate: >= 90% for diagnoses and medications in reference table
- Entity extraction recall: all diagnoses, all medications, all lab values must be captured

## Priority Order
1. ICD-10 code accuracy (most important for OMOP compliance)
2. OMOP concept ID accuracy
3. Confidence calibration (scores must reflect actual uncertainty)
4. Entity extraction completeness (no missed diagnoses or medications)
5. Language detection accuracy

## Edge Cases
| Scenario                        | Expected Behaviour                                          |
|---------------------------------|-------------------------------------------------------------|
| Mixed Bengali/Hindi note        | Set language="mixed", extract all entities from both scripts |
| Non-standard lab column names   | Map to nearest standard term, note in reasoning             |
| Drug brand name in source       | Standardize to generic, preserve brand in original_text     |
| VCF variant not in reference    | Extract with no_standard_code flag, include rsID and gene   |
| ICD-10 code ambiguous (2 valid) | Choose most specific, note alternative in reasoning         |
| Lab value outside reference     | Flag in reasoning, do not change icd10 mapping based on this|
