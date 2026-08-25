# Impact model — assumptions

This document exists so the "$1.9M/yr" figure in the README is never asserted without a paper trail. Fill in the `Source` column with a real citation (a public fintech engineering blog post, a payments industry report, etc.) before publishing.

| # | Assumption | Value used | Source (fill in before publishing) |
|---|---|---|---|
| 1 | Monthly transaction volume for a mid-size fintech | 8,000,000 | TODO — cite a public benchmark |
| 2 | Duplicate settlement rate without dedup (retries × queue redelivery) | 0.4% | TODO |
| 3 | Average transaction value | TODO | TODO |
| 4 | Cost of a duplicate settlement (reversal + support + trust cost) | TODO | TODO |

## Calculation

```
avoided_duplicates_per_month = monthly_volume * duplicate_rate
avoided_loss_per_month       = avoided_duplicates_per_month * cost_per_duplicate
avoided_loss_per_year        = avoided_loss_per_month * 12
```

## Rule for this file

Never change the README's "Modeled business impact" number without updating this file in the same commit. If a reviewer asks "how did you get this number," the answer must be "read `docs/impact-model.md`," not "I estimated it."
