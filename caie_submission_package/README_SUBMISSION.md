# CAIE Submission Package
## Computers & Industrial Engineering — Ready-to-upload files

Prepared from your manuscript:
**Integrated Sample Size Optimization and Configurable Test Chamber Scheduling under Environmental Constraints**

---

## 1. Upload these files (minimum set)

| # | File | Required? | Editorial Manager step |
|---|------|-----------|-------------------------|
| 1 | `01_title_page.docx` (or `.tex`) | **Yes** | Title page (with author details) |
| 2 | `02_manuscript_anonymized.tex` + `References.bib` | **Yes** | Anonymized manuscript |
| 3 | `03_highlights.txt` | **Yes** | Separate file; name must contain `highlights` |
| 4 | `04_declaration_competing_interests.docx` | **Yes** | Attach/upload files |
| 5 | `figures/chamber_structure.png` | **Yes** (if used) | Artwork / with manuscript |
| 6 | `05_cover_letter.docx` | Recommended | Cover letter |
| 7 | `07_data_availability_statement.txt` | **Yes** (system field) | Research data / data statement |

Also enter in the online form (not only as files):
- Abstract (≤250 words) — already in manuscript
- Keywords (1–7)
- CRediT roles — see `06_author_contributions_CRediT.txt`
- Funding — see `08_funding_statement.txt`
- Corresponding author: **Ali Ekici** (full address + phone)

---

## 2. What was changed for double-anonymized review

In `02_manuscript_anonymized.tex`:
- Author names, affiliations, and e-mails removed
- Acknowledgements removed (kept only on title page)
- Institutional identifiers removed from the introduction wording
- Data availability statement kept without author/institution names
- Bibliography trimmed to **only cited** references

**Do not** put author names anywhere in the anonymized PDF, figure files, or supplementary materials.

---

## 3. Before you submit — edit these placeholders

1. **Phone number** on title page / cover letter  
2. **Acknowledgements** (or delete the paragraph)  
3. **CRediT roles** if the draft roles are not exact  
4. **Funding** if you have a grant (replace the “no funding” sentence)  
5. **Suggested reviewers** in the cover letter  
6. Add your real figure file:
   ```
   figures/chamber_structure.png
   ```
7. If you used ChatGPT/Claude/etc. for writing (beyond grammar check), add this section **before References** in the anonymized manuscript:

   ```
   Declaration of generative AI and AI-assisted technologies in the manuscript preparation process
   During the preparation of this work the author(s) used [TOOL] in order to [REASON].
   After using this tool/service, the author(s) reviewed and edited the content as needed
   and take(s) full responsibility for the content of the published article.
   ```

---

## 4. Compile the anonymized PDF (recommended check)

```bash
cd caie_submission_package
pdflatex 02_manuscript_anonymized.tex
bibtex 02_manuscript_anonymized
pdflatex 02_manuscript_anonymized.tex
pdflatex 02_manuscript_anonymized.tex
```

Upload the **`.tex` source** (and `.bib`) as editable files. PDF alone is not accepted as the source.

If you prefer Word for the manuscript, copy the compiled content into a single-column `.docx` and remove all author identifiers again.

---

## 5. Highlights (character counts)

All bullets are ≤ 85 characters:

1. TCSSOP jointly optimizes sample sizes, chamber settings, and schedules (77)
2. Sample decisions create decision-dependent pull-down and parallelism trade-offs (80)
3. Iterative framework combines chamber assignment, sample search, and EDD scheduling (83)
4. Equal-budget FS(6) remains far worse, proving allocation—not quantity—drives gains (84)
5. Best setting uses 0.20–0.30 VNS fallback with K_max=200 across 90 scenarios (79)

Upload `03_highlights.txt` with filename containing `highlights` (e.g. `highlights.txt`).

---

## 6. Research data (Option C)

CAIE requires either:
- deposit + cite/link the dataset, **or**
- a statement explaining why data cannot be shared

Default statement in the package uses “available upon reasonable request” for synthetic instances and confidentiality for real industrial data. Stronger option: deposit synthetic instances on Mendeley Data / Zenodo and put the DOI in the data statement + reference list as `[dataset]`.

---

## 7. Submission checklist (journal)

- [ ] One corresponding author with full postal address, e-mail, phone  
- [ ] Title page separate from anonymized manuscript  
- [ ] Highlights uploaded as separate editable file  
- [ ] Competing interests Word `.docx` uploaded  
- [ ] Funding disclosed  
- [ ] CRediT contributions entered  
- [ ] Data availability statement entered  
- [ ] All figures cited and supplied as separate files if required  
- [ ] Spelling/grammar checked  
- [ ] References complete (APA-style; `apalike` used)  
- [ ] No identifying information in anonymized files  
- [ ] Copyrighted third-party material permissions obtained (if any)  
- [ ] Generative AI disclosure added only if applicable  

---

## 8. After acceptance (for later)

- Publishing agreement  
- Open access / APC decision (if chosen)  
- Proof corrections within ~2 days  

---

## File list

```
caie_submission_package/
├── README_SUBMISSION.md
├── 01_title_page.tex
├── 01_title_page.docx
├── 02_manuscript_anonymized.tex
├── References.bib
├── 03_highlights.txt
├── 04_declaration_competing_interests.docx
├── 05_cover_letter.docx
├── 06_author_contributions_CRediT.txt
├── 07_data_availability_statement.txt
├── 08_funding_statement.txt
└── figures/
    └── README_FIGURES.txt
```
