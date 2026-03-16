package tr.testodasi.heuristic;
 
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
 
public final class Main {
  private static final int VNS_PROGRESS_STEP = 200;
  private static final long DEFAULT_SEED = 42L;
  private record VnsCheckpointRow(
      int outerIteration,
      int checkpointVnsIteration,
      int maxShakes,
      int bestTotalLateness,
      int bestTotalSamples,
      int currentTotalLateness,
      int currentTotalSamples,
      long stage2ElapsedMs,
      String lastShakeKind
  ) {}

  public static void main(String[] args) {
    boolean verbose = false;
    Long randomSeed = DEFAULT_SEED;
    String dumpProjectId = null;
    boolean dumpFirst10 = false;
    String csvDir = null;
    boolean csvFlag = false;
    boolean diagnose = false;
    boolean printScheduleEvals = false;
    String batchPath = null;
    String batchOut = null;
    String batchDetailsDir = null;
    boolean batchSchedule = false;
 
    for (int idx = 0; idx < args.length; idx++) {
      String a = args[idx];
      if ("--help".equalsIgnoreCase(a) || "-h".equalsIgnoreCase(a) || "-?".equalsIgnoreCase(a)) {
        printHelp();
        return;
      }
 
      if (a != null && startsWithIgnoreCase(a, "--batch=")) {
        batchPath = a.substring("--batch=".length()).trim();
      } else if ("--batch".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          batchPath = args[idx + 1].trim();
          idx++;
        }
      }
 
      if (a != null && startsWithIgnoreCase(a, "--batchout=")) {
        batchOut = a.substring("--batchout=".length()).trim();
      } else if ("--batchOut".equalsIgnoreCase(a) || "--batchout".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          batchOut = args[idx + 1].trim();
          idx++;
        }
      }
 
      if (a != null && (startsWithIgnoreCase(a, "--batchdetails=") || startsWithIgnoreCase(a, "--batchdetaildir="))) {
        int p = a.indexOf('=');
        batchDetailsDir = p >= 0 ? a.substring(p + 1).trim() : null;
      } else if ("--batchDetails".equalsIgnoreCase(a) || "--batchDetailDir".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          batchDetailsDir = args[idx + 1].trim();
          idx++;
        }
      }
 
      if (a != null && startsWithIgnoreCase(a, "--batchschedule=")) {
        String v = a.substring("--batchschedule=".length()).trim();
        batchSchedule = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--batchSchedule".equalsIgnoreCase(a)) {
        batchSchedule = true;
      }
 
      if ("--verbose".equalsIgnoreCase(a) || "-v".equalsIgnoreCase(a)) {
        verbose = true;
      }
 
      if (a != null && a.startsWith("--dumpProject=")) {
        dumpProjectId = a.substring("--dumpProject=".length()).trim();
      }
 
      if ("--dumpFirst10".equalsIgnoreCase(a)) {
        dumpFirst10 = true;
      }
 
      if ("--diagnose".equalsIgnoreCase(a) || "--diag".equalsIgnoreCase(a)) {
        diagnose = true;
      }
 
      if ("--scheduleEvals".equalsIgnoreCase(a) || "--schedevals".equalsIgnoreCase(a)) {
        printScheduleEvals = true;
      }

      // CSV arg parsing (case-insensitive, supports both --csvDir=path and --csvDir path)
      if (a != null && startsWithIgnoreCase(a, "--csvdir=")) {
        csvDir = a.substring("--csvdir=".length()).trim();
      } else if (a != null && startsWithIgnoreCase(a, "--csv=")) {
        csvDir = a.substring("--csv=".length()).trim();
      } else if ("--csv".equalsIgnoreCase(a)) {
        csvFlag = true;
      } else if ("--csvDir".equalsIgnoreCase(a) || "--csvdir".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          csvDir = args[idx + 1].trim();
          idx++;
        } else {
          csvFlag = true;
        }
      }
 
      // deprecated flags (ignored)
      if (a != null && a.startsWith("--dispatch=")) {}
      if (a != null && a.startsWith("--mode=")) {}
      if (a != null && a.startsWith("--jobRule=")) {}
      if (a != null && a.startsWith("--jobK=")) {}
      if (a != null && a.startsWith("--atcK=")) {}
      if (a != null && a.startsWith("--orderLS=")) {}
      if (a != null && a.startsWith("--orderLSPasses=")) {}
      if (a != null && a.startsWith("--orderLSWindow=")) {}
      if (a != null && a.startsWith("--orderLSMaxEvals=")) {}
 
      if (a != null && a.startsWith("--roomLS=")) {
        String v = a.substring("--roomLS=".length()).trim();
        Data.ENABLE_ROOM_LOCAL_SEARCH = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      }
      if (a != null && a.startsWith("--roomLSMaxEvals=")) {
        try {
          Data.ROOM_LS_MAX_EVALS = Integer.parseInt(a.substring("--roomLSMaxEvals=".length()).trim());
        } catch (NumberFormatException ignored) {}
      }
      if (a != null && a.startsWith("--roomLSSwap=")) {
        String v = a.substring("--roomLSSwap=".length()).trim();
        Data.ROOM_LS_ENABLE_SWAP = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      }
      if (a != null && a.startsWith("--roomLSMove=")) {
        String v = a.substring("--roomLSMove=".length()).trim();
        Data.ROOM_LS_ENABLE_MOVE = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      }
      if (a != null && a.startsWith("--roomLSIncludeSample=")) {
        String v = a.substring("--roomLSIncludeSample=".length()).trim();
        Data.ROOM_LS_INCLUDE_SAMPLE_HEURISTIC = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      }
      if (a != null && startsWithIgnoreCase(a, "--seed=")) {
        randomSeed = parseSeedArg(a.substring("--seed=".length()), randomSeed);
      } else if ("--seed".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          randomSeed = parseSeedArg(args[idx + 1], randomSeed);
          idx++;
        }
      }
      if (a != null && a.startsWith("--validate=")) {
        String v = a.substring("--validate=".length()).trim();
        Data.ENABLE_SCHEDULE_VALIDATION = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      }
 
      if (a != null && a.startsWith("--samples=")) {
        try {
          Data.INITIAL_SAMPLES = Integer.parseInt(a.substring("--samples=".length()).trim());
        } catch (NumberFormatException ignored) {}
      } else if ("--samples".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          try {
            Data.INITIAL_SAMPLES = Integer.parseInt(args[idx + 1].trim());
            idx++;
          } catch (NumberFormatException ignored) {}
        }
      }
 
      if (a != null && a.startsWith("--sampleIncrease=")) {
        String v = a.substring("--sampleIncrease=".length()).trim();
        Data.ENABLE_SAMPLE_INCREASE = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--sampleIncrease".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length && args[idx + 1] != null && !args[idx + 1].startsWith("--")) {
          String v = args[idx + 1].trim();
          Data.ENABLE_SAMPLE_INCREASE = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
          idx++;
        } else {
          Data.ENABLE_SAMPLE_INCREASE = true;
        }
      }

      if (a != null && a.startsWith("--lsLateEarlyPairCount=")) {
        try {
          Data.LS_LATE_EARLY_PAIR_COUNT = Integer.parseInt(a.substring("--lsLateEarlyPairCount=".length()).trim());
        } catch (NumberFormatException ignored) {}
      } else if ("--lsLateEarlyPairCount".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          try {
            Data.LS_LATE_EARLY_PAIR_COUNT = Integer.parseInt(args[idx + 1].trim());
            idx++;
          } catch (NumberFormatException ignored) {}
        }
      }

      if (a != null && a.startsWith("--vnsSkipLocalSearchThreshold=")) {
        try {
          Data.VNS_SKIP_LOCAL_SEARCH_THRESHOLD =
              Double.parseDouble(a.substring("--vnsSkipLocalSearchThreshold=".length()).trim());
        } catch (NumberFormatException ignored) {}
      } else if ("--vnsSkipLocalSearchThreshold".equalsIgnoreCase(a)) {
        if (idx + 1 < args.length) {
          try {
            Data.VNS_SKIP_LOCAL_SEARCH_THRESHOLD = Double.parseDouble(args[idx + 1].trim());
            idx++;
          } catch (NumberFormatException ignored) {}
        }
      }

      if (a != null && a.startsWith("--vnsNoPlusMinusTwo=")) {
        String v = a.substring("--vnsNoPlusMinusTwo=".length()).trim();
        Data.VNS_DISABLE_PLUS_MINUS_TWO = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--vnsNoPlusMinusTwo".equalsIgnoreCase(a)) {
        Data.VNS_DISABLE_PLUS_MINUS_TWO = true;
      }

      if (a != null && a.startsWith("--lsRestrictByLateness=")) {
        String v = a.substring("--lsRestrictByLateness=".length()).trim();
        Data.LS_RESTRICT_MOVES_BY_LATENESS = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--lsRestrictByLateness".equalsIgnoreCase(a)) {
        Data.LS_RESTRICT_MOVES_BY_LATENESS = true;
      }

      if (a != null && a.startsWith("--lsLateEarlyPairing=")) {
        String v = a.substring("--lsLateEarlyPairing=".length()).trim();
        Data.LS_LATE_EARLY_PAIRING = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--lsLateEarlyPairing".equalsIgnoreCase(a)) {
        Data.LS_LATE_EARLY_PAIRING = true;
      }

      if (a != null && a.startsWith("--vnsSkipLocalSearchIfWorse=")) {
        String v = a.substring("--vnsSkipLocalSearchIfWorse=".length()).trim();
        Data.VNS_SKIP_LOCAL_SEARCH_IF_WORSE = "1".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v);
      } else if ("--vnsSkipLocalSearchIfWorse".equalsIgnoreCase(a)) {
        Data.VNS_SKIP_LOCAL_SEARCH_IF_WORSE = true;
      }
    }
 
    // Enforce global minimum samples
    if (Data.INITIAL_SAMPLES < Data.MIN_SAMPLES) {
      Data.INITIAL_SAMPLES = Data.MIN_SAMPLES;
    }
 
    if (verbose) {
      System.out.println("INFO: Effective options:");
      System.out.println("- INITIAL_SAMPLES=" + Data.INITIAL_SAMPLES + " (MIN_SAMPLES=" + Data.MIN_SAMPLES + ")");
      System.out.println("- ENABLE_SAMPLE_INCREASE=" + Data.ENABLE_SAMPLE_INCREASE);
      System.out.println("- SAMPLE_MAX=" + Data.SAMPLE_MAX + " SAMPLE_SEARCH_MAX_EVALS=" + Data.SAMPLE_SEARCH_MAX_EVALS);
      System.out.println("- SCHEDULING_MODE=" + Data.SCHEDULING_MODE + " JOB_DISPATCH_RULE=" + Data.JOB_DISPATCH_RULE);
      System.out.println("- ENABLE_ROOM_LOCAL_SEARCH=" + Data.ENABLE_ROOM_LOCAL_SEARCH);
      System.out.println("- RANDOM_SEED=" + (randomSeed == null ? "random" : randomSeed));
    }
 
    if (batchPath != null && !batchPath.isBlank()) {
      if ((batchDetailsDir == null || batchDetailsDir.isBlank()) && (csvFlag || (csvDir != null && !csvDir.isBlank()))) {
        batchDetailsDir = (csvDir == null || csvDir.isBlank()) ? "csv_out" : csvDir;
        batchSchedule = true;
        if (verbose) {
          System.out.println("INFO: Batch mode: using --csv/--csvDir as --batchDetails=" + batchDetailsDir + " and enabling --batchSchedule");
        }
      }
 
      String out = (batchOut == null || batchOut.isBlank()) ? "batch_results.csv" : batchOut;
      try {
        BatchRunner.run(
            Paths.get(batchPath),
            Paths.get(out),
            (batchDetailsDir == null || batchDetailsDir.isBlank()) ? null : Paths.get(batchDetailsDir),
            batchSchedule,
            verbose,
            printScheduleEvals
        );
      } catch (IOException e) {
        throw new RuntimeException("Failed to run batch: " + batchPath, e);
      }
      System.out.println("Batch results written to: " + Paths.get(out).toAbsolutePath());
      if (batchDetailsDir != null && !batchDetailsDir.isBlank()) {
        System.out.println("Batch details written to: " + Paths.get(batchDetailsDir).toAbsolutePath());
      }
      return;
    }
 
    Map<Integer, Integer> nextVnsCheckpointByOuter = new HashMap<>();
    Map<Integer, int[]> bestAtOuterByLatenessSamples = new HashMap<>();
    List<VnsCheckpointRow> vnsCheckpointRows = new ArrayList<>();
    HeuristicSolver.ProgressListener listener = new HeuristicSolver.ProgressListener() {
      @Override
      public void onVnsIteration(int outerIteration, int vnsIteration, String kind, int totalLateness, int totalSamples, long stage2ElapsedMs) {
        int maxShakes = Math.max(0, Data.STAGE2_MAX_SHAKES);
        if (maxShakes < VNS_PROGRESS_STEP) return;

        int[] best = bestAtOuterByLatenessSamples.get(outerIteration);
        if (best == null ||
            totalLateness < best[0] ||
            (totalLateness == best[0] && totalSamples < best[1])) {
          best = new int[]{totalLateness, totalSamples};
          bestAtOuterByLatenessSamples.put(outerIteration, best);
        }

        int nextCheckpoint = nextVnsCheckpointByOuter.getOrDefault(outerIteration, VNS_PROGRESS_STEP);
        while (vnsIteration >= nextCheckpoint && nextCheckpoint <= maxShakes) {
          VnsCheckpointRow row = new VnsCheckpointRow(
              outerIteration,
              nextCheckpoint,
              maxShakes,
              best[0],
              best[1],
              totalLateness,
              totalSamples,
              stage2ElapsedMs,
              kind
          );
          vnsCheckpointRows.add(row);
          System.out.println(
              "VNS checkpoint: outerIter=" + outerIteration +
                  " vnsIter=" + nextCheckpoint + "/" + maxShakes +
                  " bestTotalLateness=" + best[0] +
                  " bestTotalSamples=" + best[1] +
                  " currentTotalLateness=" + totalLateness +
                  " currentTotalSamples=" + totalSamples +
                  " stage2ElapsedMs=" + stage2ElapsedMs +
                  " lastShake=" + kind
          );
          nextCheckpoint += VNS_PROGRESS_STEP;
        }
        nextVnsCheckpointByOuter.put(outerIteration, nextCheckpoint);
      }
    };

    HeuristicSolver solver = buildSolver(verbose, listener, randomSeed);
    long solveT0 = System.currentTimeMillis();
    List<Solution> sols = solver.solve();
    long solveT1 = System.currentTimeMillis();
    long scheduleEvals = solver.getScheduleEvalCount();
 
    Solution best = sols.stream().min(Comparator.comparingInt(s -> s.totalLateness)).orElseThrow();
    int bestLateProjects = countLateProjects(best);
 
    for (Solution s : sols) {
      printSolution(s);
      System.out.println();
    }
 
    System.out.println("TOTAL runtimeMs = " + (solveT1 - solveT0));
    if (printScheduleEvals) {
      System.out.println("TOTAL scheduleEvals = " + scheduleEvals);
    }
 
    System.out.println("====================");
    System.out.println("BEST (min total lateness): iter=" + best.iteration +
        " totalLateness=" + best.totalLateness +
        " lateProjects=" + bestLateProjects);
    System.out.println("BEST Stage1 (setpoints) runtimeMs = " + best.stage1RuntimeMs);
 
    if (best.stage2PreShakeTotalLateness >= 0) {
      System.out.println("BEST Stage2 pre-shake: totalLateness=" + best.stage2PreShakeTotalLateness +
          " totalSamples=" + best.stage2PreShakeTotalSamples +
          " runtimeMs=" + best.stage2PreShakeRuntimeMs +
          " didShake=" + best.stage2DidShake);
    }
    if (best.stage2RuntimeMs >= 0) {
      System.out.println("BEST Stage2 runtimeMs = " + best.stage2RuntimeMs +
          " (passes=" + best.stage2Passes +
          ", evals=" + best.stage2Evals +
          ", shakes=" + best.stage2Shakes +
          ", vnsRuntimeMs=" + best.stage2VnsRuntimeMs + ")");
    }
 
    if (dumpProjectId != null && !dumpProjectId.isBlank()) {
      dumpProject(best, dumpProjectId);
    }
 
    if (dumpFirst10) {
      dumpFirstNProjects(best, 10);
    }
 
    if ((csvDir != null && !csvDir.isBlank()) || csvFlag) {
      if (csvDir == null || csvDir.isBlank()) csvDir = "csv_out";
      try {
        int rows = exportCsv(best, Paths.get(csvDir), vnsCheckpointRows);
        System.out.println();
        System.out.println("CSV exported to: " + Paths.get(csvDir).toAbsolutePath());
        System.out.println("- schedule_by_project.csv");
        System.out.println("- schedule_by_station.csv");
        System.out.println("- vns_checkpoints.csv");
        System.out.println("Rows written: " + rows);
      } catch (IOException e) {
        throw new RuntimeException("Failed to export CSV to dir=" + csvDir, e);
      }
    }
 
    if (diagnose) {
      printDiagnostics(best);
    }
  }
 
  private static int countLateProjects(Solution s) {
    int c = 0;
    for (ProjectResult r : s.results) {
      if (r.lateness > 0) c++;
    }
    return c;
  }
 
  private static void printHelp() {
    System.out.println("Heuristic scheduler (job-based EDD + room assignment)");
    System.out.println();
    System.out.println("Usage:");
    System.out.println("  java -cp <classes> tr.testodasi.heuristic.Main [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  -h, --help                 Show this help and exit");
    System.out.println("  -v, --verbose              Print effective options + extra logs");
    System.out.println("  --batch <instances.csv>    Run multiple instances and write one summary CSV");
    System.out.println("  --batchOut <out.csv>       Output path for batch summary (default: batch_results.csv)");
    System.out.println("  --batchDetails <dir>       Write detailed CSVs into <dir> (project results, chamber env)");
    System.out.println("  --batchSchedule            (with --batchDetails) also write full schedule rows (can be large)");
    System.out.println("  --dumpProject=P17          Dump detailed schedule for one project id");
    System.out.println("  --dumpFirst10              Dump detailed schedule for P1..P10");
    System.out.println("  --diagnose, --diag         Print capacity/workload diagnostics");
    System.out.println("  --scheduleEvals            Print schedule evaluation count");
    System.out.println();
    System.out.println("  --csv                      Export CSV to ./csv_out");
    System.out.println("  --csvDir <dir>             Export CSV to <dir> (creates dir if missing)");
    System.out.println("  --csvDir=<dir>             Same as above");
    System.out.println("  --csv=<dir>                Same as above");
    System.out.println("                            NOTE: In batch mode, --csv/--csvDir act as a shortcut for:");
    System.out.println("                                  --batchDetails <dir> --batchSchedule");
    System.out.println();
    System.out.println("Tuning flags (booleans accept: 1/true/yes):");
    System.out.println("  --samples=<n>              Initial samples per project (min=" + Data.MIN_SAMPLES + ")");
    System.out.println("  --samples <n>              Same as above");
    System.out.println("  --sampleIncrease           Enable sample local-search (same as true)");
    System.out.println("  --sampleIncrease <bool>    Enable/disable sample local-search");
    System.out.println("  --sampleIncrease=<bool>    Enable/disable sample local-search");
    System.out.println("  --vnsNoPlusMinusTwo=<bool> Disable +/-2 sample moves in Stage2");
    System.out.println("  --lsRestrictByLateness=<bool>  Only increase late, decrease on-time");
    System.out.println("  --lsLateEarlyPairing=<bool>    Pair late/early projects in local search");
    System.out.println("  --lsLateEarlyPairCount=<n>     Max late/early pairs (default=10)");
    System.out.println("  --vnsSkipLocalSearchIfWorse=<bool>  Skip local search if 20% worse than best");
    System.out.println("  --vnsSkipLocalSearchThreshold=<x>  Threshold ratio (default=0.20)");
    System.out.println();
    System.out.println("  --roomLS=<bool>            Enable/disable room local-search");
    System.out.println("  --roomLSMaxEvals=<n>       Room local-search evaluation budget");
    System.out.println("  --roomLSSwap=<bool>        Enable/disable swap neighbors");
    System.out.println("  --roomLSMove=<bool>        Enable/disable move neighbors");
    System.out.println("  --roomLSIncludeSample=<bool>  Score rooms using sample-heuristic too (slower)");
    System.out.println("  --seed=<n|random>          RNG seed (default=42, use random for non-deterministic runs)");
    System.out.println();
    System.out.println("  --validate=<bool>          Enable/disable schedule validation");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  java -cp out tr.testodasi.heuristic.Main --verbose --csv");
    System.out.println("  java -cp out tr.testodasi.heuristic.Main --dumpProject=P1 --csvDir=csv_out");
  }
 
  private static void printSolution(Solution s) {
    System.out.println("====================");
    System.out.println("ITERATION " + s.iteration);
    System.out.println("Total lateness = " + s.totalLateness);
    System.out.println("Late projects = " + countLateProjects(s));
    System.out.println("Stage1 (setpoints) runtimeMs = " + s.stage1RuntimeMs);
 
    if (s.stage2PreShakeTotalLateness >= 0) {
      System.out.println("Stage2 pre-shake total lateness = " + s.stage2PreShakeTotalLateness +
          " (totalSamples=" + s.stage2PreShakeTotalSamples +
          ", runtimeMs=" + s.stage2PreShakeRuntimeMs +
          ", didShake=" + s.stage2DidShake + ")");
    }
    if (s.stage2RuntimeMs >= 0) {
      System.out.println("Stage2 runtimeMs = " + s.stage2RuntimeMs +
          " (passes=" + s.stage2Passes +
          ", evals=" + s.stage2Evals +
          ", shakes=" + s.stage2Shakes +
          ", vnsRuntimeMs=" + s.stage2VnsRuntimeMs + ")");
    }
 
    System.out.println("\nChamber setpoints (for this iteration):");
    for (var c : Data.CHAMBERS) {
      Env env = s.chamberEnv.get(c.id);
      System.out.println("- " + c.id + " stations=" + c.stations + " volt=" + c.voltageCapable + " humAdj=" + c.humidityAdjustable + " => " + env);
    }
 
    System.out.println("\nProject sample counts:");
    s.projects.stream()
        .sorted(Comparator.comparing(p -> p.id))
        .forEach(p -> System.out.println("- " + p.id + " samples=" + p.samples + " due=" + p.dueDateDays + " needsVolt=" + p.needsVoltage));
 
    System.out.println("\nProject results:");
    s.results.stream()
        .sorted(Comparator.comparing(r -> r.projectId))
        .forEach(r -> System.out.println("- " + r.projectId + " completion=" + r.completionDay + " due=" + r.dueDate + " lateness=" + r.lateness));
  }
 
  private static void dumpProject(Solution sol, String projectId) {
    Objects.requireNonNull(sol);
    Objects.requireNonNull(projectId);
    System.out.println();
    System.out.println("====================");
    System.out.println("SCHEDULE DUMP for " + projectId + " (iter=" + sol.iteration + ")");
    sol.schedule.stream()
        .filter(j -> projectId.equals(j.projectId))
        .sorted(Comparator.comparingInt((Scheduler.ScheduledJob j) -> j.start).thenComparing(j -> j.testId))
        .forEach(j -> System.out.println(
            j.testId + " " + j.env +
                " sample=" + j.sampleIdx +
                " " + j.chamberId + "[st" + j.stationIdx + "]" +
                " start=" + j.start + " end=" + j.end
        ));
  }
 
  private static void dumpFirstNProjects(Solution sol, int n) {
    Objects.requireNonNull(sol);
    if (n <= 0) return;
 
    Set<String> wanted = new TreeSet<>((a, b) -> Integer.compare(parseProjectNum(a), parseProjectNum(b)));
    for (int i = 1; i <= n; i++) wanted.add("P" + i);
 
    System.out.println();
    System.out.println("====================");
    System.out.println("FIRST " + n + " PROJECTS - DETAILED SCHEDULE (iter=" + sol.iteration + ")");
 
    for (String pid : wanted) {
      System.out.println();
      System.out.println("---- " + pid + " ----");
      sol.schedule.stream()
          .filter(j -> pid.equals(j.projectId))
          .sorted(Comparator.comparingInt((Scheduler.ScheduledJob j) -> j.start).thenComparing(j -> j.testId))
          .forEach(j -> System.out.println(
              j.testId +
                  " env=" + j.env +
                  " sample=" + j.sampleIdx +
                  " room=" + j.chamberId +
                  " station=" + j.stationIdx +
                  " start=" + j.start +
                  " end=" + j.end
          ));
    }
  }
 
  private static int parseProjectNum(String pid) {
    if (pid == null) return Integer.MAX_VALUE;
    if (!pid.startsWith("P")) return Integer.MAX_VALUE;
    try {
      return Integer.parseInt(pid.substring(1));
    } catch (NumberFormatException e) {
      return Integer.MAX_VALUE;
    }
  }
 
  private static int exportCsv(Solution sol, Path dir, List<VnsCheckpointRow> vnsCheckpoints) throws IOException {
    Objects.requireNonNull(sol);
    Objects.requireNonNull(dir);
    Objects.requireNonNull(vnsCheckpoints);
    Files.createDirectories(dir);
 
    Map<String, Project> projectById = new HashMap<>();
    for (Project p : sol.projects) projectById.put(p.id, p);
 
    Path byProject = dir.resolve("schedule_by_project.csv");
    try (BufferedWriter w = Files.newBufferedWriter(byProject)) {
      w.write("iteration,projectId,testId,category,tempC,humidity,durationDays,needsVoltage,dueDateDays,samples,sampleIdx,chamberId,stationIdx,startDay,endDay");
      w.newLine();
      sol.schedule.stream()
          .sorted(Comparator.comparing((Scheduler.ScheduledJob j) -> j.projectId)
              .thenComparingInt(j -> j.start)
              .thenComparing(j -> j.testId))
          .forEach(j -> writeRow(w, sol.iteration, j, projectById.get(j.projectId)));
    }
 
    Path byStation = dir.resolve("schedule_by_station.csv");
    try (BufferedWriter w = Files.newBufferedWriter(byStation)) {
      w.write("iteration,chamberId,stationIdx,startDay,endDay,projectId,testId,category,tempC,humidity,durationDays,needsVoltage,dueDateDays,samples,sampleIdx");
      w.newLine();
      sol.schedule.stream()
          .sorted(Comparator.comparing((Scheduler.ScheduledJob j) -> j.chamberId)
              .thenComparingInt(j -> j.stationIdx)
              .thenComparingInt(j -> j.start)
              .thenComparing(j -> j.projectId)
              .thenComparing(j -> j.testId))
          .forEach(j -> writeRowStation(w, sol.iteration, j, projectById.get(j.projectId)));
    }

    Path vnsCheckpointsPath = dir.resolve("vns_checkpoints.csv");
    try (BufferedWriter w = Files.newBufferedWriter(vnsCheckpointsPath)) {
      w.write("outerIteration,checkpointVnsIteration,maxShakes,bestTotalLateness,bestTotalSamples,currentTotalLateness,currentTotalSamples,stage2ElapsedMs,lastShakeKind");
      w.newLine();
      vnsCheckpoints.stream()
          .sorted(Comparator.comparingInt((VnsCheckpointRow r) -> r.outerIteration)
              .thenComparingInt(r -> r.checkpointVnsIteration))
          .forEach(r -> {
            try {
              w.write(
                  r.outerIteration + "," +
                      r.checkpointVnsIteration + "," +
                      r.maxShakes + "," +
                      r.bestTotalLateness + "," +
                      r.bestTotalSamples + "," +
                      r.currentTotalLateness + "," +
                      r.currentTotalSamples + "," +
                      r.stage2ElapsedMs + "," +
                      (r.lastShakeKind == null ? "" : r.lastShakeKind)
              );
              w.newLine();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
 
    return sol.schedule.size();
  }
 
  private static void writeRow(BufferedWriter w, int iteration, Scheduler.ScheduledJob j, Project p) {
    try {
      w.write(iteration + "," + j.projectId + "," + j.testId + "," + j.category + "," +
          j.env.temperatureC + "," + j.env.humidity + "," + j.durationDays + "," +
          (p != null && p.needsVoltage) + "," + (p != null ? p.dueDateDays : "") + "," + (p != null ? p.samples : "") + "," +
          j.sampleIdx + "," + j.chamberId + "," + j.stationIdx + "," + j.start + "," + j.end);
      w.newLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
 
  private static void writeRowStation(BufferedWriter w, int iteration, Scheduler.ScheduledJob j, Project p) {
    try {
      w.write(iteration + "," + j.chamberId + "," + j.stationIdx + "," + j.start + "," + j.end + "," +
          j.projectId + "," + j.testId + "," + j.category + "," +
          j.env.temperatureC + "," + j.env.humidity + "," + j.durationDays + "," +
          (p != null && p.needsVoltage) + "," + (p != null ? p.dueDateDays : "") + "," + (p != null ? p.samples : "") + "," +
          j.sampleIdx);
      w.newLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
 
  private static boolean startsWithIgnoreCase(String s, String prefix) {
    if (s == null || prefix == null) return false;
    if (s.length() < prefix.length()) return false;
    return s.regionMatches(true, 0, prefix, 0, prefix.length());
  }

  private static Long parseSeedArg(String raw, Long current) {
    if (raw == null) return current;
    String v = raw.trim();
    if (v.isEmpty()) return current;
    if ("random".equalsIgnoreCase(v) || "none".equalsIgnoreCase(v) || "null".equalsIgnoreCase(v)) {
      return null;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException ignored) {
      return current;
    }
  }

  private static HeuristicSolver buildSolver(
      boolean verbose,
      HeuristicSolver.ProgressListener listener,
      Long randomSeed
  ) {
    try {
      return HeuristicSolver.class
          .getConstructor(boolean.class, HeuristicSolver.ProgressListener.class, Long.class)
          .newInstance(verbose, listener, randomSeed);
    } catch (ReflectiveOperationException ignored) {
      // Backward compatibility: older solver versions only have (boolean, listener) constructor.
      return new HeuristicSolver(verbose, listener);
    }
  }
 
  private static void printDiagnostics(Solution best) {
    System.out.println();
    System.out.println("====================");
    System.out.println("DIAGNOSTICS");
 
    Map<Env, Integer> stationsByEnv = new HashMap<>();
    for (ChamberSpec c : Data.CHAMBERS) {
      Env env = best.chamberEnv.get(c.id);
      if (env == null) continue;
      stationsByEnv.merge(env, c.stations, Integer::sum);
    }
 
    Map<Env, Long> workByEnv = new HashMap<>();
    for (Project p : best.projects) {
      for (int ti = 0; ti < Data.TESTS.size(); ti++) {
        if (!p.required[ti]) continue;
        TestDef t = Data.TESTS.get(ti);
        int jobs = 1;
        if (t.category == TestCategory.PULLDOWN) jobs = p.samples;
        workByEnv.merge(t.env, (long) jobs * t.durationDays, Long::sum);
      }
    }
 
    System.out.println("ENV workload / station:");
    workByEnv.entrySet().stream()
        .sorted((a, b) -> {
          double ra = a.getValue() / (stationsByEnv.getOrDefault(a.getKey(), 1) * 1.0);
          double rb = b.getValue() / (stationsByEnv.getOrDefault(b.getKey(), 1) * 1.0);
          return Double.compare(rb, ra);
        })
        .forEach(e -> {
          int cap = stationsByEnv.getOrDefault(e.getKey(), 0);
          double ratio = cap == 0 ? Double.POSITIVE_INFINITY : (e.getValue() / (double) cap);
          System.out.println("- " + e.getKey() + " workDays=" + e.getValue() + " stations=" + cap + " workDaysPerStation=" + String.format("%.2f", ratio));
        });
  }
 
  private static String fmt(double x) {
    return String.format(Locale.ROOT, "%.4f", x);
  }
}