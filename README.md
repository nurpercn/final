# RCSP-SO Instances

This repository contains the generated instances used in the computational study of RCSP-SO.

The instance file is `instances/instances_demo.csv`. It contains 90 instances. Each instance has 50 products, so the file contains 4,500 product rows. The 90 instances are the full combination of 10 test scenarios, 3 due-date scenarios, and 3 voltage scenarios.

## Repository structure

```text
rcsp-so-instances/
├── README.md
├── LICENSE
└── instances/
    └── instances_demo.csv
```

## Instance set

| Factor | Levels | Count |
|---|---|---:|
| Test scenario | 1–10 | 10 |
| Due-date scenario | 1, 2, 3 | 3 |
| Voltage scenario | 1, 2, 3 | 3 |
| Instances | 10 × 3 × 3 | 90 |
| Products per instance | P1–P50 | 50 |

The instance name is `{testScenarioId}_{dueScenarioId}_{voltScenarioId}`. For example, `6_1_3` is test scenario 6, due-date scenario 1, and voltage scenario 3.

For a given test scenario and due-date scenario, the three voltage scenarios use the same due dates. Their test requirements also match, except for one cell: in instance `1_1_1`, product `P1` requires `EE_25`, while the same product in `1_1_2` and `1_1_3` does not.

### Voltage scenarios

`needsVolt = 1` means the product requires nonstandard voltage. Every instance in a voltage scenario has the same number of such products.

| Voltage scenario | Products requiring nonstandard voltage | Share |
|---:|---:|---:|
| 1 | 10 of 50 | 20% |
| 2 | 14 of 50 | 28% |
| 3 | 22 of 50 | 44% |

### Due-date scenarios

Every instance in a due-date scenario uses due dates in the same range.

| Due-date scenario | Due dates (days) |
|---:|---|
| 1 | 42–57 |
| 2 | 30–69 |
| 3 | 34–69 |

## File format

`instances/instances_demo.csv` is a semicolon-separated CSV file with one row per product. The file is UTF-8 and begins with a byte-order mark, as produced by the spreadsheet export.

```text
instanceId;projectId;testScenarioId;dueScenarioId;voltScenarioId;dueDateDays;needsVolt;GasAmount;Pulldown;EE_32;EE_25;EE_16;Perf_25;Perf_43;Perf_38;Perf_32;Perf_16;Perf_10;FreezingC;TemperatureRise;CU_10;CU_25;CU_32
```

| Column | Meaning |
|---|---|
| `instanceId` | Instance name, `{testScenarioId}_{dueScenarioId}_{voltScenarioId}`. |
| `projectId` | Product id inside the instance (`P1`–`P50`). |
| `testScenarioId` | Test-requirement scenario of the instance. |
| `dueScenarioId` | Due-date scenario of the instance. |
| `voltScenarioId` | Voltage scenario of the instance. |
| `dueDateDays` | Due date of the product, in days. |
| `needsVolt` | `1` if the product requires nonstandard voltage, `0` otherwise. |
| Remaining columns | Test requirements. `1` means the product requires the test, `0` means it does not. |

## Test columns

| Column | Test |
|---|---|
| `GasAmount` | Gas amount determination at 43°C |
| `Pulldown` | Pulldown at 43°C |
| `EE_32` | Energy efficiency at 32°C |
| `EE_25` | Energy efficiency at 25°C |
| `EE_16` | Energy efficiency at 16°C |
| `Perf_25` | Performance at 25°C |
| `Perf_43` | Performance at 43°C |
| `Perf_38` | Performance at 38°C |
| `Perf_32` | Performance at 32°C |
| `Perf_16` | Performance at 16°C |
| `Perf_10` | Performance at 10°C |
| `FreezingC` | Freezing capacity at 25°C |
| `TemperatureRise` | Temperature rise at 25°C |
| `CU_10` | Consumer usage at 10°C |
| `CU_25` | Consumer usage at 25°C and 85% relative humidity |
| `CU_32` | Consumer usage at 32°C and 85% relative humidity |

In every row, `GasAmount`, `Pulldown`, `Perf_25`, `CU_10`, `CU_25`, and `CU_32` are required. Exactly one of `Perf_43` and `Perf_38` is required. `FreezingC` and `TemperatureRise` are either both required or both omitted. Each product requires one or two of `EE_32`, `EE_25`, and `EE_16`, except product `P1` in instance `1_1_1`, which requires all three.

Test durations, chamber data, and the other fixed parameters of the model are not stored in the instance file.

## Citation

If you use these instances, please cite the related study.

```bibtex
@article{,
  title   = {},
  author  = {},
  journal = {},
  year    = {}
}
```

## License

The instance file is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See [LICENSE](LICENSE).
