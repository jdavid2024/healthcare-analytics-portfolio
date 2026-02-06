# Tableau Calculations – Technical Appendix

This section documents key Tableau calculations used in the reporting pipeline.  
The goal is to ensure accurate aggregation, consistent denominators, and reproducible healthcare survey analytics.

These calculations support automated reporting dashboards built from REDCap → Snowflake → Tableau.

---

## Level of Detail (LOD) Calculations

LOD expressions are used to control denominators and prevent filter distortion when calculating survey percentages.

Healthcare reporting often requires excluding “Not applicable” or “Don’t know” responses from denominators. FIXED LOD ensures percent calculations remain stable regardless of dashboard filters.

### Percent of “Yes, definitely”

