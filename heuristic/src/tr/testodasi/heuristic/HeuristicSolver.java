
package tr.testodasi.heuristic;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class HeuristicSolver {
  public static final Long DEFAULT_RANDOM_SEED = 42L;

  private final Scheduler scheduler = new Scheduler();
  private final boolean verbose;
  private final ProgressListener listener;
  private final Long randomSeed;
  private Random seededRandom;

  public HeuristicSolver() {
    this(false);
  }

  public HeuristicSolver(boolean verbose) {
    this(verbose, null, DEFAULT_RANDOM_SEED);
  }

  public HeuristicSolver(boolean verbose, ProgressListener listener) {
    this(verbose, listener, DEFAULT_RANDOM_SEED);
  }

  public HeuristicSolver(boolean verbose, ProgressListener listener, Long randomSeed) {
    this.verbose = verbose;
    this.listener = listener;
    this.randomSeed = randomSeed;
  }

  public List<Solution> solve() {
    List<Project> projects = Data.buildProjects(Data.INITIAL_SAMPLES);
    return solveWithProjects(projects);
  }

  /** Runs solver using the given project list (for batch/CSV instances). */
  public List<Solution> solveWithProjects(List<Project> projects) {
    Objects.requireNonNull(projects);

    scheduler.resetEvalCount();
    seededRandom = randomSeed != null ? new Random(randomSeed) : null;
    List<Solution> solutions = new ArrayList<>();
    Map<String, Env> prevRoom = null;

    List<Project> current = deepCopy(projects);

    for (int iter = 1; iter <= 10; iter++) {
      final long tStage1Start = System.nanoTime();
      Map<String, Env> room = stage1_assignRooms(current);
      final long stage1RuntimeMs = (System.nanoTime() - tStage1Start) / 1_000_000L;
      if (listener != null) listener.onStage1Done(iter, stage1RuntimeMs);

      if (Data.ENABLE_ROOM_LOCAL_SEARCH) {
        RoomScore base = scoreRoom(room, current);
        Map<String, Env> improvedRoom = improveRoomsByLocalSearch(current, room, base.totalLateness);
        RoomScore after = scoreRoom(improvedRoom, current);
        if (after.totalLateness < base.totalLateness) {
          if (verbose) {
            System.out.println("INFO: Room local-search improved total lateness: " +
                base.totalLateness + " -> " + after.totalLateness);
          }
          room = improvedRoom;
        } else if (verbose) {
          System.out.println("INFO: Room local-search no improvement (baseline=" + base.totalLateness + ")");
        }
      }

      // Eğer oda setleri artık değişmiyorsa sabit noktaya geldik: tekrar üretmek yerine dur.
      if (prevRoom != null && prevRoom.equals(room)) {
        if (verbose) {
          System.out.println("INFO: Stage3 converged (room set unchanged). Stopping at iter=" + (iter - 1));
        }
        break;
      }

      // Stage2: EDD scheduling + sample artırma
      Stage2Result stage2 = Data.ENABLE_SAMPLE_INCREASE ? stage2_increaseSamples(iter, room, current, true) : Stage2Result.identity(deepCopy(current));
      List<Project> improved = stage2.projects;
      Scheduler.EvalResult eval = scheduler.evaluateNoCopy(improved, room);

      solutions.add(new Solution(
          iter,
          eval.totalLateness,
          deepCopy(improved),
          room,
          eval.projectResults,
          eval.schedule,
          stage1RuntimeMs,
          stage2.preShakeTotalLateness,
          stage2.preShakeTotalSamples,
          stage2.preShakeRuntimeMs,
          stage2.didShake,
          stage2.runtimeMs,
          stage2.passes,
          stage2.evals,
          stage2.shakes,
          stage2.vnsRuntimeMs
      ));

      if (Data.ENABLE_SCHEDULE_VALIDATION) {
        List<String> violations = Scheduler.validateSchedule(improved, room, eval.schedule);
        if (!violations.isEmpty()) {
          StringBuilder sb = new StringBuilder();
          sb.append("Schedule validation failed (").append(violations.size()).append(" violations). First 20:\n");
          for (int i = 0; i < Math.min(20, violations.size()); i++) {
            sb.append("- ").append(violations.get(i)).append("\n");
          }
          throw new IllegalStateException(sb.toString());
        }
      }

      prevRoom = room;
      current = deepCopy(improved);
    }

    return solutions;
  }

  public long getScheduleEvalCount() {
    return scheduler.getEvalCount();
  }

  private record RoomScore(int totalLateness) {}
  private record ChamberLoad(ChamberSpec chamber, int busyDays, double normalizedBusyDays) {}

  private RoomScore scoreRoom(Map<String, Env> room, List<Project> projects) {
    if (Data.ROOM_LS_INCLUDE_SAMPLE_HEURISTIC) {
      // When used as a scoring subroutine inside room local-search, DO NOT emit progress logs.
      Stage2Result improved = stage2_increaseSamples(0, room, projects, false);
      Scheduler.EvalResult eval = scheduler.evaluateFastNoCopy(improved.projects, room);
      return new RoomScore(eval.totalLateness);
    }
    Scheduler.EvalResult eval = scheduler.evaluateFastNoCopy(projects, room);
    return new RoomScore(eval.totalLateness);
  }

  private Map<String, Env> improveRoomsByLocalSearch(List<Project> projects, Map<String, Env> startRoom, int baseline) {
    int maxEvals = Math.max(10, Data.ROOM_LS_MAX_EVALS);
    Map<String, Env> best = new LinkedHashMap<>(startRoom);
    int bestScore = baseline;

    // demanded env set from current projects
    Set<Env> demanded = new LinkedHashSet<>();
    Set<Env> demandedByVolt = new LinkedHashSet<>();
    for (Project p : projects) {
      for (int ti = 0; ti < Data.TESTS.size(); ti++) {
        if (!p.required[ti]) continue;
        Env env = Data.TESTS.get(ti).env;
        demanded.add(env);
        if (p.needsVoltage) demandedByVolt.add(env);
      }
    }
    if (demanded.isEmpty()) return best;

    int evals = 0;
    boolean improvedAny;
    do {
      improvedAny = false;
      List<ChamberLoad> denseOrder = rankChambersByLoad(projects, best);
      List<ChamberLoad> sparseOrder = new ArrayList<>(denseOrder);
      sparseOrder.sort(
          Comparator.comparingDouble((ChamberLoad x) -> x.normalizedBusyDays)
              .thenComparingInt(x -> x.busyDays)
              .thenComparing(x -> x.chamber.id)
      );

      // SWAP: en yoğun oda ile en boş oda çiftlerini öncelikle dene.
      if (Data.ROOM_LS_ENABLE_SWAP) {
        for (ChamberLoad dense : denseOrder) {
          if (evals >= maxEvals) break;
          for (ChamberLoad sparse : sparseOrder) {
            if (evals >= maxEvals) break;
            if (dense.chamber.id.equals(sparse.chamber.id)) continue;
            if (dense.normalizedBusyDays <= sparse.normalizedBusyDays) continue;
            Env denseEnv = best.get(dense.chamber.id);
            Env sparseEnv = best.get(sparse.chamber.id);
            if (denseEnv == null || sparseEnv == null || denseEnv.equals(sparseEnv)) continue;
            if (!canAssign(dense.chamber, sparseEnv) || !canAssign(sparse.chamber, denseEnv)) continue;

            Map<String, Env> candRoom = new LinkedHashMap<>(best);
            candRoom.put(dense.chamber.id, sparseEnv);
            candRoom.put(sparse.chamber.id, denseEnv);
            if (!isRoomFeasible(candRoom, demanded, demandedByVolt)) continue;
            evals++;
            int s = scoreRoom(candRoom, projects).totalLateness;
            if (s < bestScore) {
              best = candRoom;
              bestScore = s;
              improvedAny = true;
              break;
            }
          }
          if (improvedAny) break;
        }
      }

      // MOVE: en boş odanın ayarını en yoğun odanın env değerine taşı.
      if (!improvedAny && Data.ROOM_LS_ENABLE_MOVE) {
        for (ChamberLoad dense : denseOrder) {
          if (evals >= maxEvals) break;
          Env denseEnv = best.get(dense.chamber.id);
          if (denseEnv == null) continue;
          for (ChamberLoad sparse : sparseOrder) {
            if (evals >= maxEvals) break;
            if (dense.chamber.id.equals(sparse.chamber.id)) continue;
            if (dense.normalizedBusyDays <= sparse.normalizedBusyDays) continue;
            Env sparseEnv = best.get(sparse.chamber.id);
            if (sparseEnv == null || denseEnv.equals(sparseEnv)) continue;
            if (!canAssign(sparse.chamber, denseEnv)) continue;
            Map<String, Env> candRoom = new LinkedHashMap<>(best);
            candRoom.put(sparse.chamber.id, denseEnv);
            if (!isRoomFeasible(candRoom, demanded, demandedByVolt)) continue;
            evals++;
            int s = scoreRoom(candRoom, projects).totalLateness;
            if (s < bestScore) {
              best = candRoom;
              bestScore = s;
              improvedAny = true;
              break;
            }
          }
          if (improvedAny) break;
        }
      }

    } while (improvedAny && evals < maxEvals);

    if (verbose) {
      System.out.println("INFO: Room local-search evals=" + evals + " best=" + bestScore + " baseline=" + baseline);
    }
    return best;
  }

  private static boolean canAssign(ChamberSpec chamber, Env env) {
    if (env.humidity == Humidity.H85 && !chamber.humidityAdjustable) return false;
    return true;
  }

  private List<ChamberLoad> rankChambersByLoad(List<Project> projects, Map<String, Env> room) {
    Scheduler.EvalResult eval = scheduler.evaluateNoCopy(projects, room);
    Map<String, Integer> busyDaysByChamber = new HashMap<>();
    for (Scheduler.ScheduledJob j : eval.schedule) {
      busyDaysByChamber.merge(j.chamberId, j.durationDays, Integer::sum);
    }
    List<ChamberLoad> out = new ArrayList<>(Data.CHAMBERS.size());
    for (ChamberSpec c : Data.CHAMBERS) {
      int busyDays = busyDaysByChamber.getOrDefault(c.id, 0);
      double normalized = busyDays / (double) Math.max(1, c.stations);
      out.add(new ChamberLoad(c, busyDays, normalized));
    }
    out.sort(
        Comparator.comparingDouble((ChamberLoad x) -> x.normalizedBusyDays).reversed()
            .thenComparingInt(x -> x.busyDays).reversed()
            .thenComparing(x -> x.chamber.id)
    );
    return out;
  }

  private static boolean isRoomFeasible(Map<String, Env> room, Set<Env> demanded, Set<Env> demandedByVolt) {
    // (1) each demanded env has at least 1 room
    Map<Env, Integer> count = new HashMap<>();
    Map<Env, Integer> countVoltRooms = new HashMap<>();
    Map<String, ChamberSpec> chamberById = new HashMap<>();
    for (ChamberSpec c : Data.CHAMBERS) chamberById.put(c.id, c);

    for (var e : room.entrySet()) {
      Env env = e.getValue();
      if (env == null) continue;
      ChamberSpec ch = chamberById.get(e.getKey());
      if (ch == null) continue;
      // humidity feasibility
      if (env.humidity == Humidity.H85 && !ch.humidityAdjustable) return false;
      count.merge(env, 1, Integer::sum);
      if (ch.voltageCapable) countVoltRooms.merge(env, 1, Integer::sum);
    }
    for (Env env : demanded) {
      if (count.getOrDefault(env, 0) <= 0) return false;
    }
    // (2) env demanded by voltage projects should have at least 1 voltage-capable room
    for (Env env : demandedByVolt) {
      if (countVoltRooms.getOrDefault(env, 0) <= 0) return false;
    }
    return true;
  }

  private record Stage2Result(
      List<Project> projects,
      int preShakeTotalLateness,
      int preShakeTotalSamples,
      long preShakeRuntimeMs,
      boolean didShake,
      long runtimeMs,
      int passes,
      int evals,
      int shakes,
      long vnsRuntimeMs
  ) {
    static Stage2Result identity(List<Project> projects) {
      int ts = totalSamples(projects);
      return new Stage2Result(projects, -1, ts, -1L, false, -1L, -1, -1, 0, 0L);
    }
  }

  private static final class LatenessInfo {
    final int[] lateness;
    final int[] earlySlack;

    private LatenessInfo(int[] lateness, int[] earlySlack) {
      this.lateness = lateness;
      this.earlySlack = earlySlack;
    }

    boolean isLate(int idx) {
      return lateness[idx] > 0;
    }

    static LatenessInfo fromResults(List<Project> projects, Map<String, Integer> idxById, List<ProjectResult> results) {
      int n = projects.size();
      int[] late = new int[n];
      int[] slack = new int[n];
      for (ProjectResult r : results) {
        Integer idx = idxById.get(r.projectId);
        if (idx == null) continue;
        int completion = r.completionDay;
        int due = r.dueDate;
        int l = Math.max(0, completion - due);
        int s = Math.max(0, due - completion);
        late[idx] = l;
        slack[idx] = s;
      }
      return new LatenessInfo(late, slack);
    }
  }

  private Stage2Result stage2_increaseSamples(int outerIteration, Map<String, Env> room, List<Project> startProjects, boolean emitProgress) {
    List<Project> current = deepCopy(startProjects);

    // enforce minimum samples
    for (Project p : current) {
      if (p.samples < Data.MIN_SAMPLES) p.samples = Data.MIN_SAMPLES;
    }

    final long tStart = System.nanoTime();

    boolean needsProjectResults = Data.LS_RESTRICT_MOVES_BY_LATENESS || Data.LS_LATE_EARLY_PAIRING;
    Map<String, Integer> idxById = null;
    if (needsProjectResults) {
      idxById = new HashMap<>();
      for (int i = 0; i < current.size(); i++) idxById.put(current.get(i).id, i);
    }

    Scheduler.EvalResult baseEval = needsProjectResults
        ? scheduler.evaluateResultsNoScheduleNoCopy(current, room)
        : scheduler.evaluateFastNoCopy(current, room);
    LatenessInfo latenessInfo = needsProjectResults
        ? LatenessInfo.fromResults(current, idxById, baseEval.projectResults)
        : null;
    if (verbose) {
      System.out.println("INFO: Stage2 initial total lateness = " + baseEval.totalLateness);
    }

    // Keep best solution encountered (because we may apply "shake" moves that can worsen temporarily).
    List<Project> bestOverall = deepCopy(current);
    Scheduler.EvalResult bestOverallEval = baseEval;
    int bestOverallTotalSamples = totalSamples(bestOverall);

    int evalBudget = Math.max(500, Data.SAMPLE_SEARCH_MAX_EVALS);
    int evals = 0;
    int passes = 0;
    int shakes = 0;
    boolean nextShakeDown = true; // alternate: down -> up -> down ...
    String lastShakeKind = null;
    boolean vnsEndEmittedForLastShake = true; // no shake yet => treat as emitted

    boolean capturedPreShake = false;
    int preShakeTotalLateness = -1;
    int preShakeTotalSamples = -1;
    long preShakeRuntimeMs = -1L;
    boolean didAnyShake = false;

    // Per-project local search: try +1, +2, -1, -2 and accept best (min lateness).
    // Tie-break: if lateness equal, prefer fewer samples (keeps solution minimal).
    while (evals < evalBudget) {
      boolean improvedAny = false;
      passes++;

      boolean skipLocalSearch = false;
      if (Data.VNS_SKIP_LOCAL_SEARCH_IF_WORSE) {
        if (bestOverallEval.totalLateness == 0) {
          skipLocalSearch = baseEval.totalLateness > 0;
        } else {
          double limit = bestOverallEval.totalLateness * (1.0 + Data.VNS_SKIP_LOCAL_SEARCH_THRESHOLD);
          skipLocalSearch = baseEval.totalLateness > limit;
        }
      }

      if (!skipLocalSearch) {
        for (int i = 0; i < current.size() && evals < evalBudget; i++) {
          Project p0 = current.get(i);
          int curSamples = p0.samples;

          int bestSamples = curSamples;
          Scheduler.EvalResult bestEval = baseEval;

          // Before first VNS shake, keep +/-2 neighborhood active.
          // If configured, disable +/-2 after VNS is entered (post-shake phase).
          boolean disablePlusMinusTwoNow = Data.VNS_DISABLE_PLUS_MINUS_TWO && capturedPreShake;
          int[] deltas;
          if (disablePlusMinusTwoNow) {
            deltas = (curSamples <= Data.MIN_SAMPLES)
                ? new int[]{+1}
                : new int[]{+1, -1};
          } else {
            deltas = (curSamples <= Data.MIN_SAMPLES)
                ? new int[]{+1, +2}
                : new int[]{+1, +2, -1, -2};
          }

          boolean restrict = Data.LS_RESTRICT_MOVES_BY_LATENESS && latenessInfo != null;
          boolean isLate = restrict && latenessInfo.isLate(i);

          for (int d : deltas) {
            if (evals >= evalBudget) break;
            if (restrict) {
              if (isLate && d < 0) continue;
              if (!isLate && d > 0) continue;
            }
            int ns = curSamples + d;
            if (ns < Data.MIN_SAMPLES) continue;
            if (ns > Data.SAMPLE_MAX) continue;
            if (ns == curSamples) continue;

            p0.samples = ns;
            Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(current, room);
            evals++;
            p0.samples = curSamples;

            if (e.totalLateness < bestEval.totalLateness ||
                (e.totalLateness == bestEval.totalLateness && ns < bestSamples)) {
              bestEval = e;
              bestSamples = ns;
            }
          }

          boolean accept =
              bestEval.totalLateness < baseEval.totalLateness ||
                  (bestEval.totalLateness == baseEval.totalLateness && bestSamples < curSamples);

          if (accept && bestSamples != curSamples) {
            p0.samples = bestSamples;
            baseEval = needsProjectResults
                ? scheduler.evaluateResultsNoScheduleNoCopy(current, room)
                : bestEval;
            if (needsProjectResults) {
              latenessInfo = LatenessInfo.fromResults(current, idxById, baseEval.projectResults);
            }
            improvedAny = true;
            if (verbose) {
              System.out.println("INFO: Stage2 accept sample move => " + p0.id +
                  " " + curSamples + " -> " + bestSamples +
                  " totalLateness=" + baseEval.totalLateness);
            }
          }

          // track global best
          if (baseEval.totalLateness < bestOverallEval.totalLateness ||
              (baseEval.totalLateness == bestOverallEval.totalLateness && totalSamples(current) < bestOverallTotalSamples)) {
            bestOverall = deepCopy(current);
            bestOverallEval = baseEval;
            bestOverallTotalSamples = totalSamples(bestOverall);
          }
        }
      }

      // If single-project moves stall, try 2-project exchange moves:
      // one project +1 sample while another project -1 sample (keeps total samples constant).
      if (!skipLocalSearch && !improvedAny && evals < evalBudget) {
        if (Data.LS_LATE_EARLY_PAIRING && latenessInfo != null) {
          int bestLateIdx = -1;
          int bestEarlyIdx = -1;
          Scheduler.EvalResult bestPairEval = baseEval;

          List<Integer> lateIdx = new ArrayList<>();
          List<Integer> earlyIdx = new ArrayList<>();
          for (int i = 0; i < current.size(); i++) {
            if (latenessInfo.lateness[i] > 0) {
              lateIdx.add(i);
            } else if (latenessInfo.earlySlack[i] > 0) {
              earlyIdx.add(i);
            }
          }

          if (!lateIdx.isEmpty() && !earlyIdx.isEmpty()) {
            final int[] lateArr = latenessInfo.lateness;
            final int[] slackArr = latenessInfo.earlySlack;
            lateIdx.sort((a, b) -> Integer.compare(lateArr[b], lateArr[a]));
            earlyIdx.sort((a, b) -> Integer.compare(slackArr[b], slackArr[a]));

            int k = Math.min(Data.LS_LATE_EARLY_PAIR_COUNT, Math.min(lateIdx.size(), earlyIdx.size()));
            for (int t = 0; t < k && evals < evalBudget; t++) {
              int i = lateIdx.get(t);
              int j = earlyIdx.get(t);
              Project pi = current.get(i);
              Project pj = current.get(j);
              if (pi.samples + 1 > Data.SAMPLE_MAX) continue;
              if (pj.samples - 1 < Data.MIN_SAMPLES) continue;

              pi.samples += 1;
              pj.samples -= 1;
              Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(current, room);
              evals++;
              pi.samples -= 1;
              pj.samples += 1;

              if (e.totalLateness < bestPairEval.totalLateness) {
                bestPairEval = e;
                bestLateIdx = i;
                bestEarlyIdx = j;
              }
            }
          }

          if (bestLateIdx >= 0 && bestEarlyIdx >= 0 && bestPairEval.totalLateness < baseEval.totalLateness) {
            Project pi = current.get(bestLateIdx);
            Project pj = current.get(bestEarlyIdx);
            int si0 = pi.samples;
            int sj0 = pj.samples;
            pi.samples = si0 + 1;
            pj.samples = sj0 - 1;
            baseEval = needsProjectResults
                ? scheduler.evaluateResultsNoScheduleNoCopy(current, room)
                : bestPairEval;
            if (needsProjectResults) {
              latenessInfo = LatenessInfo.fromResults(current, idxById, baseEval.projectResults);
            }
            improvedAny = true;
            if (verbose) {
              System.out.println("INFO: Stage2 accept late/early pair => " +
                  pi.id + " " + si0 + " -> " + pi.samples + ", " +
                  pj.id + " " + sj0 + " -> " + pj.samples +
                  " totalLateness=" + baseEval.totalLateness);
            }

            // track global best
            if (baseEval.totalLateness < bestOverallEval.totalLateness ||
                (baseEval.totalLateness == bestOverallEval.totalLateness && totalSamples(current) < bestOverallTotalSamples)) {
              bestOverall = deepCopy(current);
              bestOverallEval = baseEval;
              bestOverallTotalSamples = totalSamples(bestOverall);
            }
          }
        } else {
          int bestI = -1;
          int bestJ = -1;
          boolean bestDir = true; // true => i+1, j-1 ; false => i-1, j+1
          Scheduler.EvalResult bestPairEval = baseEval;

          for (int i = 0; i < current.size() && evals < evalBudget; i++) {
            for (int j = i + 1; j < current.size() && evals < evalBudget; j++) {
              Project pi = current.get(i);
              Project pj = current.get(j);
              int si = pi.samples;
              int sj = pj.samples;

              // Direction 1: i+1, j-1
              if (si + 1 <= Data.SAMPLE_MAX && sj - 1 >= Data.MIN_SAMPLES) {
                pi.samples = si + 1;
                pj.samples = sj - 1;
                Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(current, room);
                evals++;
                pi.samples = si;
                pj.samples = sj;
                if (e.totalLateness < bestPairEval.totalLateness) {
                  bestPairEval = e;
                  bestI = i;
                  bestJ = j;
                  bestDir = true;
                }
              }

              // Direction 2: i-1, j+1
              if (evals >= evalBudget) break;
              if (si - 1 >= Data.MIN_SAMPLES && sj + 1 <= Data.SAMPLE_MAX) {
                pi.samples = si - 1;
                pj.samples = sj + 1;
                Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(current, room);
                evals++;
                pi.samples = si;
                pj.samples = sj;
                if (e.totalLateness < bestPairEval.totalLateness) {
                  bestPairEval = e;
                  bestI = i;
                  bestJ = j;
                  bestDir = false;
                }
              }
            }
          }

          if (bestI >= 0 && bestJ >= 0 && bestPairEval.totalLateness < baseEval.totalLateness) {
            Project pi = current.get(bestI);
            Project pj = current.get(bestJ);
            int si0 = pi.samples;
            int sj0 = pj.samples;
            if (bestDir) {
              pi.samples = si0 + 1;
              pj.samples = sj0 - 1;
            } else {
              pi.samples = si0 - 1;
              pj.samples = sj0 + 1;
            }
            baseEval = needsProjectResults
                ? scheduler.evaluateResultsNoScheduleNoCopy(current, room)
                : bestPairEval;
            if (needsProjectResults) {
              latenessInfo = LatenessInfo.fromResults(current, idxById, baseEval.projectResults);
            }
            improvedAny = true;
            if (verbose) {
              System.out.println("INFO: Stage2 accept pair exchange => " +
                  pi.id + " " + si0 + " -> " + pi.samples + ", " +
                  pj.id + " " + sj0 + " -> " + pj.samples +
                  " totalLateness=" + baseEval.totalLateness);
            }

            // track global best
            if (baseEval.totalLateness < bestOverallEval.totalLateness ||
                (baseEval.totalLateness == bestOverallEval.totalLateness && totalSamples(current) < bestOverallTotalSamples)) {
              bestOverall = deepCopy(current);
              bestOverallEval = baseEval;
              bestOverallTotalSamples = totalSamples(bestOverall);
            }
          }
        }
      }

      // If still no improvement, apply "shake" to escape local optimum:
      // - First: randomly set 40% of projects to MIN_SAMPLES (2)
      // - Next: randomly set 40% of projects to 6 (bounded by SAMPLE_MAX)
      if (!improvedAny && evals < evalBudget) {
        // If we've already applied a shake before, reaching this point means we hit a new local optimum
        // AFTER the last shake (i.e., "end of VNS iteration"). Emit progress here (not immediately after shake).
        if (emitProgress && listener != null && capturedPreShake && shakes > 0 && !vnsEndEmittedForLastShake) {
          long elapsedMs = (System.nanoTime() - tStart) / 1_000_000L;
          listener.onVnsIteration(outerIteration, shakes, lastShakeKind, baseEval.totalLateness, totalSamples(current), elapsedMs);
          vnsEndEmittedForLastShake = true;
        }

        // Capture the local optimum BEFORE applying the first shake (VNS baseline).
        if (!capturedPreShake) {
          capturedPreShake = true;
          preShakeTotalLateness = baseEval.totalLateness;
          preShakeTotalSamples = totalSamples(current);
          preShakeRuntimeMs = (System.nanoTime() - tStart) / 1_000_000L;
          if (verbose) {
            System.out.println("INFO: Stage2 pre-shake local optimum => totalLateness=" + preShakeTotalLateness +
                " totalSamples=" + preShakeTotalSamples +
                " runtimeMs=" + preShakeRuntimeMs);
          }
          if (emitProgress && listener != null) {
            listener.onStage2PreShake(outerIteration, preShakeTotalLateness, preShakeTotalSamples, preShakeRuntimeMs);
          }
        }

        boolean didShake = false;
        String shakeKind = null;
        if (shakes < Data.STAGE2_MAX_SHAKES ) { // safety
          if (nextShakeDown) {
            didShake = tryShakeDownToMin(room, current, baseEval, evalBudget, evals);
            shakeKind = didShake ? "DOWN" : null;
          } else {
            didShake = tryShakeUpToSix(room, current, baseEval, evalBudget, evals);
            shakeKind = didShake ? "UP" : null;
          }
        }

        if (didShake) {
          // tryShake* mutates current/baseEval via returned holder
          baseEval = needsProjectResults
              ? scheduler.evaluateResultsNoScheduleNoCopy(current, room)
              : lastShakeEval;
          if (needsProjectResults) {
            latenessInfo = LatenessInfo.fromResults(current, idxById, baseEval.projectResults);
          }
          evals = lastShakeEvals;
          improvedAny = true;
          shakes++;
          didAnyShake = true;
          lastShakeKind = shakeKind;
          vnsEndEmittedForLastShake = false;
          nextShakeDown = !nextShakeDown;

          if (baseEval.totalLateness < bestOverallEval.totalLateness ||
              (baseEval.totalLateness == bestOverallEval.totalLateness && totalSamples(current) < bestOverallTotalSamples)) {
            bestOverall = deepCopy(current);
            bestOverallEval = baseEval;
            bestOverallTotalSamples = totalSamples(bestOverall);
          }
        }
      }

      if (!improvedAny) break;
      if (passes > 200000) break; // safety
    }

    // If we performed at least one shake but never hit another stall (e.g., budget/safety cutoff),
    // emit the current state as the "end of last VNS iteration" so logs/CSV are not missing the final VNS result.
    if (emitProgress && listener != null && capturedPreShake && shakes > 0 && !vnsEndEmittedForLastShake) {
      long elapsedMs = (System.nanoTime() - tStart) / 1_000_000L;
      listener.onVnsIteration(outerIteration, shakes, lastShakeKind, baseEval.totalLateness, totalSamples(current), elapsedMs);
      vnsEndEmittedForLastShake = true;
    }

    if (verbose) {
      System.out.println("INFO: Stage2 sample-search passes=" + passes + " evals=" + evals + " budget=" + evalBudget +
          " finalTotal=" + baseEval.totalLateness);
    }

    final long runtimeMs = (System.nanoTime() - tStart) / 1_000_000L;
    if (!capturedPreShake) {
      // If we never stalled enough to even attempt shake, use final state as "pre-shake" baseline.
      preShakeTotalLateness = baseEval.totalLateness;
      preShakeTotalSamples = totalSamples(current);
      preShakeRuntimeMs = runtimeMs;
      if (verbose) {
        System.out.println("INFO: Stage2 pre-shake baseline not reached (no shake attempt). Using final state.");
      }
      if (emitProgress && listener != null) {
        listener.onStage2PreShake(outerIteration, preShakeTotalLateness, preShakeTotalSamples, preShakeRuntimeMs);
      }
    }

    long vnsRuntimeMs = didAnyShake ? Math.max(0L, runtimeMs - preShakeRuntimeMs) : 0L;
    return new Stage2Result(bestOverall, preShakeTotalLateness, preShakeTotalSamples, preShakeRuntimeMs, didAnyShake, runtimeMs, passes, evals, shakes, vnsRuntimeMs);
  }

  // ---- Stage2 helpers (shake moves) ----

  // These two fields are a small workaround to avoid threading an object through the loop.
  // They are only used inside stage2_increaseSamples call chain.
  private Scheduler.EvalResult lastShakeEval;
  private int lastShakeEvals;

  private static int totalSamples(List<Project> ps) {
    int sum = 0;
    for (Project p : ps) sum += p.samples;
    return sum;
  }

  private int nextRandomInt(int boundExclusive) {
    if (boundExclusive <= 0) {
      throw new IllegalArgumentException("boundExclusive must be > 0");
    }
    if (seededRandom != null) {
      return seededRandom.nextInt(boundExclusive);
    }
    return ThreadLocalRandom.current().nextInt(boundExclusive);
  }

  private boolean tryShakeDownToMin(
      Map<String, Env> room,
      List<Project> current,
      Scheduler.EvalResult baseEval,
      int evalBudget,
      int evals
  ) {
    if (evals >= evalBudget) return false;
    int k = Math.max(1, (int) Math.ceil(current.size() * 0.30));

    List<Integer> idxs = new ArrayList<>();
    for (int i = 0; i < current.size(); i++) {
      if (current.get(i).samples > Data.MIN_SAMPLES) idxs.add(i);
    }
    if (idxs.isEmpty()) return false;
    // Randomly select projects (uniform, without replacement).
    for (int i = idxs.size() - 1; i > 0; i--) {
      int j = nextRandomInt(i + 1);
      int tmp = idxs.get(i);
      idxs.set(i, idxs.get(j));
      idxs.set(j, tmp);
    }

    List<Project> cand = deepCopy(current);
    int changed = 0;
    for (int t = 0; t < idxs.size() && changed < k; t++) {
      int i = idxs.get(t);
      if (cand.get(i).samples > Data.MIN_SAMPLES) {
        cand.get(i).samples = Data.MIN_SAMPLES;
        changed++;
      }
    }
    if (changed == 0) return false;

    Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(cand, room);
    evals++;
    current.clear();
    current.addAll(cand);
    lastShakeEval = e;
    lastShakeEvals = evals;
    if (verbose) {
      System.out.println("INFO: Stage2 shake DOWN => set " + changed + " projects to " + Data.MIN_SAMPLES +
          " totalLateness=" + e.totalLateness);
    }
    return true;
  }

  private boolean tryShakeUpToSix(
      Map<String, Env> room,
      List<Project> current,
      Scheduler.EvalResult baseEval,
      int evalBudget,
      int evals
  ) {
    if (evals >= evalBudget) return false;
    int target = Math.min(6, Data.SAMPLE_MAX);
    if (target < Data.MIN_SAMPLES) target = Data.MIN_SAMPLES;
    int k = Math.max(1, (int) Math.ceil(current.size() * 0.30));

    List<Integer> idxs = new ArrayList<>();
    for (int i = 0; i < current.size(); i++) {
      if (current.get(i).samples < target) idxs.add(i);
    }
    if (idxs.isEmpty()) return false;
    // Randomly select projects (uniform, without replacement).
    for (int i = idxs.size() - 1; i > 0; i--) {
      int j = nextRandomInt(i + 1);
      int tmp = idxs.get(i);
      idxs.set(i, idxs.get(j));
      idxs.set(j, tmp);
    }

    List<Project> cand = deepCopy(current);
    int changed = 0;
    for (int t = 0; t < idxs.size() && changed < k; t++) {
      int i = idxs.get(t);
      if (cand.get(i).samples < target) {
        cand.get(i).samples = target;
        changed++;
      }
    }
    if (changed == 0) return false;

    Scheduler.EvalResult e = scheduler.evaluateFastNoCopy(cand, room);
    evals++;
    current.clear();
    current.addAll(cand);
    lastShakeEval = e;
    lastShakeEvals = evals;
    if (verbose) {
      System.out.println("INFO: Stage2 shake UP => set " + changed + " projects to " + target +
          " totalLateness=" + e.totalLateness);
    }
    return true;
  }

  /**
   * Aşama 1: Oda set değerlerini belirle (sıcaklık/nem sabit kalır).
   *
   * Amaç:
   * - Tüm iş yükünü (env bazında jobDays) ODALAR arasında tek bir global dengeleme ile dağıt.
   * - Voltaj gerektiren iş yükü için voltaj-capable odalarda env kapasitesi ayır (en az 1 volt oda / volt-env).
   * - 85% nem isteyen env sadece humAdj odalara atanabilir.
   *
   * Not: Burada job'ları tek tek odaya atamıyoruz; her oda 1 env'e sabitleniyor.
   * Scheduler daha sonra bu oda/env set değerleri üzerinde job'ları istasyonlara yerleştiriyor.
   */
  private Map<String, Env> stage1_assignRooms(List<Project> projects) {
    Objects.requireNonNull(projects);

    Map<Env, Long> demandTotal = new HashMap<>();
    Map<Env, Long> demandVolt = new HashMap<>();

    // İş listesi -> env bazlı toplam iş yükü (jobCount * durationDays).
    // Bu set sadece gerçekten talep olan env'leri içerir (dengeyi bozan gereksiz atamaları engeller).
    Set<Env> demandedEnvs = new LinkedHashSet<>();

    for (Project p : projects) {
      for (int ti = 0; ti < Data.TESTS.size(); ti++) {
        if (!p.required[ti]) continue;
        TestDef t = Data.TESTS.get(ti);

        int jobs = 1;
        if (t.category == TestCategory.PULLDOWN) jobs = p.samples;

        long w = (long) jobs * (long) t.durationDays;
        // Pulldown için ekstra ağırlık kullanılmıyor.
        demandTotal.merge(t.env, w, Long::sum);
        if (p.needsVoltage) demandVolt.merge(t.env, w, Long::sum);
        demandedEnvs.add(t.env);
      }
    }

    if (demandedEnvs.isEmpty()) {
      throw new IllegalStateException("No demanded environments; check project test matrix.");
    }

    // Hangi env'lerde voltaj talebi var?
    Set<Env> demandedVoltEnvs = new LinkedHashSet<>();
    for (Env env : demandedEnvs) {
      if (demandVolt.getOrDefault(env, 0L) > 0) demandedVoltEnvs.add(env);
    }

    // Total/volt station budgets (for proportional targets)
    int totalStationsAll = 0;
    int totalStationsVoltCapable = 0;
    for (ChamberSpec c : Data.CHAMBERS) {
      totalStationsAll += c.stations;
      if (c.voltageCapable) totalStationsVoltCapable += c.stations;
    }
    long sumDemandTotal = 0;
    long sumDemandVolt = 0;
    for (Env env : demandedEnvs) sumDemandTotal += Math.max(0L, demandTotal.getOrDefault(env, 0L));
    for (Env env : demandedVoltEnvs) sumDemandVolt += Math.max(0L, demandVolt.getOrDefault(env, 0L));
    if (sumDemandTotal <= 0) {
      throw new IllegalStateException("Total demand is zero; cannot assign rooms.");
    }

    Map<Env, Double> targetStationsTotal = new HashMap<>();
    for (Env env : demandedEnvs) {
      double frac = demandTotal.getOrDefault(env, 0L) / (double) sumDemandTotal;
      targetStationsTotal.put(env, frac * totalStationsAll);
    }
    Map<Env, Double> targetStationsVolt = new HashMap<>();
    for (Env env : demandedEnvs) {
      if (sumDemandVolt <= 0 || demandVolt.getOrDefault(env, 0L) <= 0) {
        targetStationsVolt.put(env, 0.0);
      } else {
        double frac = demandVolt.getOrDefault(env, 0L) / (double) sumDemandVolt;
        targetStationsVolt.put(env, frac * totalStationsVoltCapable);
      }
    }

    // Exact (mathematical) assignment: solve the balancing objective optimally under constraints,
    // then let Stage-3 local search further improve true schedule objective if enabled.
    List<Env> envList = new ArrayList<>(demandedEnvs);
    envList.sort(Comparator.comparingInt((Env e) -> e.temperatureC).thenComparing(e -> e.humidity.toString()));

    boolean[] demandedVolt = new boolean[envList.size()];
    double[] tTot = new double[envList.size()];
    double[] tVolt = new double[envList.size()];
    for (int i = 0; i < envList.size(); i++) {
      Env e = envList.get(i);
      demandedVolt[i] = demandedVoltEnvs.contains(e);
      tTot[i] = targetStationsTotal.getOrDefault(e, 0.0);
      tVolt[i] = targetStationsVolt.getOrDefault(e, 0.0);
    }

    double wVolt = 2.0;
    return RoomAssignmentOptimizer.solveExact(Data.CHAMBERS, envList, demandedVolt, tTot, tVolt, wVolt);
  }

  private static double deltaObjectiveIfAssign(
      Env env,
      ChamberSpec chamber,
      Map<Env, Double> targetStationsTotal,
      Map<Env, Double> targetStationsVolt,
      Map<Env, Integer> assignedStationsTotal,
      Map<Env, Integer> assignedStationsVolt
  ) {
    // Objective = SSE(total stations vs target) + wVolt * SSE(volt stations vs targetVolt)
    // Delta computed only for the chosen env (others unchanged).
    double wVolt = 2.0;

    double tTot = targetStationsTotal.getOrDefault(env, 0.0);
    int aTot = assignedStationsTotal.getOrDefault(env, 0);
    double beforeTot = aTot - tTot;
    double afterTot = (aTot + chamber.stations) - tTot;
    double delta = (afterTot * afterTot) - (beforeTot * beforeTot);

    if (chamber.voltageCapable) {
      double tV = targetStationsVolt.getOrDefault(env, 0.0);
      int aV = assignedStationsVolt.getOrDefault(env, 0);
      double beforeV = aV - tV;
      double afterV = (aV + chamber.stations) - tV;
      delta += wVolt * ((afterV * afterV) - (beforeV * beforeV));
    }
    return delta;
  }

  private static void assign(
      Map<String, Env> assignment,
      ChamberSpec chamber,
      Env env,
      Map<Env, Integer> assignedStationsTotal,
      Map<Env, Integer> assignedStationsVolt,
      Map<Env, Integer> roomCount,
      Map<Env, Integer> voltRoomCount
  ) {
    assignment.put(chamber.id, env);
    assignedStationsTotal.merge(env, chamber.stations, Integer::sum);
    roomCount.merge(env, 1, Integer::sum);
    if (chamber.voltageCapable) {
      assignedStationsVolt.merge(env, chamber.stations, Integer::sum);
      voltRoomCount.merge(env, 1, Integer::sum);
    }
  }

  private static List<Project> deepCopy(List<Project> ps) {
    List<Project> out = new ArrayList<>();
    for (Project p : ps) out.add(p.copy());
    return out;
  }

  public interface ProgressListener {
    /** Stage1 (setpoint assignment) completed for given outer iteration. */
    default void onStage1Done(int outerIteration, long stage1RuntimeMs) {}

    /** Stage2 pre-shake local optimum captured for given outer iteration. */
    default void onStage2PreShake(int outerIteration, int totalLateness, int totalSamples, long runtimeMs) {}

    /** One VNS (shake) iteration completed for given outer iteration. */
    default void onVnsIteration(int outerIteration, int vnsIteration, String kind, int totalLateness, int totalSamples, long stage2ElapsedMs) {}
  }

}