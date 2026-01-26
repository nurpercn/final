package tr.testodasi.heuristic;

import java.util.List;
import java.util.Map;

public final class Solution {
  public final int iteration;
  public final int totalLateness;
  public final List<Project> projects; // includes final samples
  public final Map<String, Env> chamberEnv; // chamberId -> assigned env
  public final List<ProjectResult> results;
  public final List<Scheduler.ScheduledJob> schedule;

  /** Stage1 (room setpoint assignment) runtime in milliseconds for this outer iteration. */
  public final long stage1RuntimeMs;

  /**
   * Stage2 (sample search) metrics to compare VNS ("shake") effect.
   *
   * <p>preShake* values represent the first local optimum reached BEFORE applying the first shake.
   * If no shake was attempted (no stall), these values may equal the final Stage2 state.
   *
   * <p>All *_Ms values are milliseconds and refer to Stage2 execution only (not whole program).
   * A value of -1 means "not recorded".
   */
  public final int stage2PreShakeTotalLateness;
  public final int stage2PreShakeTotalSamples;
  public final long stage2PreShakeRuntimeMs;
  public final boolean stage2DidShake;
  public final long stage2RuntimeMs;

  /** Stage2 search counters (includes both local-search evals and shake evals). */
  public final int stage2Passes;
  public final int stage2Evals;
  public final int stage2Shakes;

  /**
   * Stage2 time spent AFTER the pre-shake local optimum was captured (i.e., "VNS time").
   * If no shake was attempted, this is 0.
   */
  public final long stage2VnsRuntimeMs;

  public Solution(
      int iteration,
      int totalLateness,
      List<Project> projects,
      Map<String, Env> chamberEnv,
      List<ProjectResult> results,
      List<Scheduler.ScheduledJob> schedule,
      long stage1RuntimeMs,
      int stage2PreShakeTotalLateness,
      int stage2PreShakeTotalSamples,
      long stage2PreShakeRuntimeMs,
      boolean stage2DidShake,
      long stage2RuntimeMs,
      int stage2Passes,
      int stage2Evals,
      int stage2Shakes,
      long stage2VnsRuntimeMs
  ) {
    this.iteration = iteration;
    this.totalLateness = totalLateness;
    this.projects = projects;
    this.chamberEnv = chamberEnv;
    this.results = results;
    this.schedule = schedule;
    this.stage1RuntimeMs = stage1RuntimeMs;
    this.stage2PreShakeTotalLateness = stage2PreShakeTotalLateness;
    this.stage2PreShakeTotalSamples = stage2PreShakeTotalSamples;
    this.stage2PreShakeRuntimeMs = stage2PreShakeRuntimeMs;
    this.stage2DidShake = stage2DidShake;
    this.stage2RuntimeMs = stage2RuntimeMs;
    this.stage2Passes = stage2Passes;
    this.stage2Evals = stage2Evals;
    this.stage2Shakes = stage2Shakes;
    this.stage2VnsRuntimeMs = stage2VnsRuntimeMs;
  }
}