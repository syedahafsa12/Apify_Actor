# src/lib/scoring.py
from rapidfuzz import fuzz
import re

def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()

def keyword_hits(skills, haystack):
    h = " " + normalize(haystack) + " "
    hits = 0
    for s in skills or []:
        s = normalize(s)
        if not s: 
            continue
        if f" {s} " in h:
            hits += 1
    return hits

def hybrid_score(cv_title: str, cv_skills, job_title: str, job_snippet: str) -> int:
    jt = normalize(job_title)
    js = normalize(job_snippet)
    ct = normalize(cv_title)

    # Fuzzy similarity between titles
    title_sim = fuzz.token_set_ratio(ct, jt)  # 0-100

    # Skills keyword coverage
    skill_hit_count = keyword_hits(cv_skills, f"{jt} {js}")
    coverage = 0 if not cv_skills else min(100, int(100 * (skill_hit_count / max(1, len(cv_skills)))))

    # Blend with small bias for title match
    score = int(0.6 * title_sim + 0.4 * coverage)
    return max(0, min(100, score))