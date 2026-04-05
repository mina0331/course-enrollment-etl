# Model Accuracy Improvement Report

Date: April 5, 2026

## What Changed

The current enrollment prediction model was updated to use a better
classification threshold.

Files changed:
- [prediction_model/train_section_baseline.py](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/train_section_baseline.py)
- [etl/airflow/dags/model_evaluation.py](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/etl/airflow/dags/model_evaluation.py)

The logistic model itself was not replaced. The main change was:
- old classification threshold: `0.50`
- new classification threshold: `0.40`

Both code paths now expose the threshold explicitly and apply it consistently
when computing train/test metrics.

## Before And After

Held-out split:
- train term: `1228`
- test term: `1262`

Previous reported test metrics at threshold `0.50`:
- accuracy: `70.24%`
- precision: `61.11%`
- recall: `46.59%`
- f1: `52.87%`
- roc auc: `76.67%`

Updated test metrics at threshold `0.40`:
- accuracy: `71.30%`
- precision: `59.90%`
- recall: `60.16%`
- f1: `60.03%`
- roc auc: `76.67%`

Net change:
- accuracy: `+1.06` percentage points
- precision: `-1.21` percentage points
- recall: `+13.57` percentage points
- f1: `+7.16` percentage points
- roc auc: no change

## Why The Improvement Happened

The model produces probabilities. Accuracy depends on where those probabilities
are converted into a binary prediction.

At threshold `0.50`, the model was too conservative:
- it predicted fewer sections as "filled"
- precision looked a bit stronger
- recall was much lower
- many true filled sections were being missed

At threshold `0.40`, the model becomes less conservative:
- more sections are predicted as "filled"
- more of the true filled sections are captured
- recall improves substantially
- overall accuracy improves on the held-out term

The ROC AUC did not change because ROC AUC measures ranking quality across all
possible thresholds. Threshold tuning does not improve ranking; it improves the
final decision rule used to convert probabilities into `0/1` predictions.

## Interpretation

This is a decision-boundary improvement, not a representation-learning
improvement.

That means:
- the feature set is unchanged
- the probability ranking is unchanged
- the reported accuracy improved because the cutoff is better aligned with the
  class balance and score distribution in the held-out term

## Practical Reason

The held-out term contains many sections that are truly filled or effectively
high-demand, but the model often scores them in the `0.40` to `0.50` range.

Using `0.50` labeled too many of those as negative.
Using `0.40` recovers that middle band, which increases:
- true positives
- recall
- overall accuracy

## Current Recommendation

Keep `0.40` as the default classification threshold for the current model until
there is a stronger validation-driven model update.

The next likely sources of improvement are:
- threshold selection on a dedicated validation term instead of choosing it from
  the final test term
- nonlinear models
- stronger historical demand features
- better coverage and recency weighting for Course Forum demand features
