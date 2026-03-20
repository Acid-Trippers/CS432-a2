# Implementation Plan - Pre-Classification Pipeline Overhaul

This plan outlines the changes required to establish the exact pre-classification pipeline for Assignment 2, as requested by the user.

## Proposed Changes

### Configuration Updates

#### [MODIFY] [config.py](file:///Users/pranjal/Projects/CS432-a2/src/config.py)
- Refactor file path constants to match the 7 active pre-classification files.
- Remove redundant file paths (`NORMALIZED_DATA_FILE`, `FIELD_METADATA_FILE`).
- Change `BUFFER_FILE` extension to `.json`.
- Add `METADATA_FILE` (for `metadata.json`).
- Rename `METADATA_MANAGER_FILE` to `ANALYZED_SCHEMA_FILE` (for `analyzed_schema.json`).

### Logic Updates in Scripts

#### [MODIFY] [cleaner.py](file:///Users/pranjal/Projects/CS432-a2/src/cleaner.py)
- Import `BUFFER_FILE`.
- Update logic to compare records against `INITIAL_SCHEMA_FILE`.
- **Padding**: Ensure missing schema fields are padded with `null`.
- **Quarantine**: Rip out any nested objects or fields not in the schema and save them to `buffer.json` along with their parent ID.
- Ensure output is saved to `CLEANED_DATA_FILE`.

#### [MODIFY] [analyzer.py](file:///Users/pranjal/Projects/CS432-a2/src/analyzer.py)
- Update code to use `ANALYZED_SCHEMA_FILE` instead of `METADATA_MANAGER_FILE`.
- Ensure output aligns with "Empirical statistical profile".

#### [NEW] [validation.py](file:///Users/pranjal/Projects/CS432-a2/src/validation.py)
- Create script to merge structure from `INITIAL_SCHEMA_FILE` with stats from `ANALYZED_SCHEMA_FILE`.
- Output the consolidated result to `METADATA_FILE` (`metadata.json`).

#### [MODIFY] [classifier.py](file:///Users/pranjal/Projects/CS432-a2/src/classifier.py)
- Update to read only from `METADATA_FILE` (`metadata.json`).
- Update internal data mapping to use the merged metadata.

#### [MODIFY] [main.py](file:///Users/pranjal/Projects/CS432-a2/main.py)
- Update the list of files to clean in the `initialise` command.
- Ensure the pipeline sequence includes the new `validation` step.

### Cleanup

- Remove any hardcoded paths in the above scripts.
- Ensure consistent imports from `config`.

## Verification Plan

### Automated Steps
1. **Pipeline Execution**: Run the pipeline sequentially and verify file creation.
   ```bash
   python main.py initialise 100
   ```
2. **File Validation**: Check if each of the 7 files exists and has the expected format (JSON/Text).
   - `data/initial_schema.json`
   - `data/counter.txt`
   - `data/received_data.json`
   - `data/cleaned_data.json`
   - `data/buffer.json`
   - `data/analyzed_schema.json`
   - `data/metadata.json`
3. **Content Check**:
   - Verify `cleaned_data.json` has padded `null` values for missing schema fields.
   - Verify `buffer.json` contains "ripped out" fields.
   - Verify `metadata.json` is a merge of structure and stats.

### Manual Verification
- Review the content of `buffer.json` to ensure parent IDs are correctly tracked.
- Confirm [classifier.py](file:///Users/pranjal/Projects/CS432-a2/src/classifier.py) runs correctly using only `metadata.json`.
