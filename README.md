# RCSP-SO Instances

This repository contains the generated instances used in the computational study of RCSP-SO.

The instance file has 11 instances. Each instance has 50 products, so the file contains 550 product rows. An instance is identified by a test scenario, a due-date scenario, and a voltage scenario.

## Repository structure

```text
rcsp-so-instances/
├── README.md
├── LICENSE
└── instances/
    └── instances.csv
```

## Instance set

| Instance | Test scenario | Due-date scenario | Voltage scenario | Products | Due dates (days) | Products needing nonstandard voltage |
|---|---:|---:|---:|---:|---|---:|
| 2_3_2 | 2 | 3 | 2 | 50 | 34–69 | 14 |
| 2_3_3 | 2 | 3 | 3 | 50 | 34–69 | 22 |
| 3_2_2 | 3 | 2 | 2 | 50 | 30–69 | 14 |
| 5_3_1 | 5 | 3 | 1 | 50 | 34–69 | 10 |
| 6_1_3 | 6 | 1 | 3 | 50 | 42–57 | 22 |
| 7_3_1 | 7 | 3 | 1 | 50 | 34–69 | 10 |
| 7_3_3 | 7 | 3 | 3 | 50 | 34–69 | 22 |
| 8_3_1 | 8 | 3 | 1 | 50 | 34–69 | 10 |
| 9_3_1 | 9 | 3 | 1 | 50 | 34–69 | 10 |
| 10_3_2 | 10 | 3 | 2 | 50 | 34–69 | 14 |
| 10_3_3 | 10 | 3 | 3 | 50 | 34–69 | 22 |

When two instances share the same test scenario and the same due-date scenario, they have the same test requirements and the same due dates. Only the nonstandard-voltage assignments differ. This holds for `2_3_2` and `2_3_3`, for `7_3_1` and `7_3_3`, and for `10_3_2` and `10_3_3`.

## File format

`instances/instances.csv` is a semicolon-separated CSV file with one row per product. The file is UTF-8 and begins with a byte-order mark, as produced by the spreadsheet export.

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

In every row, `GasAmount`, `Pulldown`, `Perf_25`, `CU_10`, `CU_25`, and `CU_32` are required. Exactly one of `Perf_43` and `Perf_38` is required. `FreezingC` and `TemperatureRise` are either both required or both omitted. Each product requires one or two of `EE_32`, `EE_25`, and `EE_16`.

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
