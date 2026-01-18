# Research Brief: iPhoto to Photos Library Migration - File Differences

## Question

When macOS Photos migrates a library from iPhoto, and both libraries contain what appear to be the same edited photo (e.g., a crop), what are the possible differences between the two files?

## Scenario

- iPhoto Library path: `/iPhoto Library.photolibrary/Previews/YYYY/MM/DD/YYYYMMDD-HHMMSS/filename.jpg`
- Photos Library path: `/Photos Library.photoslibrary/Previews/YYYY/MM/DD/YYYYMMDD-HHMMSS/filename.jpg`

Both files:
- Have identical dimensions (e.g., 3049x2396)
- Have very similar file sizes (e.g., both 2.01 MB)
- Represent the same edit (e.g., same crop) of the same original photo
- Are NOT byte-identical (different file hashes)

## Research Questions

1. **What is stored in the "Previews" folder?**
   - Are these rendered versions of edited photos, or cached previews?
   - Does Photos re-render edits from edit history, or copy existing preview files?

2. **Why would they not be byte-identical?**
   - Does Photos re-encode JPEGs with different settings?
   - Are EXIF timestamps modified during migration?
   - Could there be minor resampling or color profile changes?
   - Is JPEG quality level different?

3. **Which version should be preferred for deduplication?**
   - Is one more authoritative or higher quality?
   - Are there known issues with iPhoto vs Photos rendering?
   - Does Photos Library preserve more information?

4. **What are reliable detection heuristics?**
   - Can I safely assume: same dimensions + same base filename + one from each library = duplicates from migration?
   - Should I systematically prefer Photos Library over iPhoto Library?
   - Are there edge cases where both should be kept?

## Desired Output

- Technical explanation of how Photos migration handles preview files
- List of specific differences that cause non-identical files (EXIF, encoding, etc.)
- Recommendation for which version to prefer
- Safe heuristic for automatic deduplication of migration duplicates
