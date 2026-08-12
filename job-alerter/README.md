# Job Alerter — CV eşleşmeli günlük iş arama + e-posta

Nur Percin profiline göre **Regulatory Affairs / Product Compliance / Certification / Market Access** ilanlarını UK job board’larından tarar, CV anahtar kelimeleriyle skorlar, sadece **aktif başvuru kabul eden** ve daha önce mail atılmamış ilanları günlük e-posta olarak gönderir.

## Kaynaklar

| Kaynak | Yöntem | Not |
|--------|--------|-----|
| **Reed** | Resmi API | Ücretsiz API key gerekir |
| **Adzuna** | Resmi API | Indeed benzeri UK aggregator; ücretsiz key |
| **Indeed UK** | RSS | Key yok; bazen engellenebilir |
| **Totaljobs** | HTML | Best-effort |
| **CV-Library** | HTML | Best-effort |
| **Glassdoor** | HTML | Best-effort / bot koruması olabilir |
| **LinkedIn Jobs** | Guest HTML | Kırılgan; dry-run çıktısına manuel arama linkleri de eklenir |

Hedef firmalar (beyaz eşya, HVAC, industrial, sertifikasyon kuruluşları) `config/profile.yaml` içinde.

## Kurulum

```bash
cd job-alerter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 1) API anahtarları (önerilir)

- Reed: https://www.reed.co.uk/developers → `REED_API_KEY`
- Adzuna: https://developer.adzuna.com/ → `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`

### 2) E-posta (Outlook / Hotmail)

`.env` içinde:

```env
ALERT_TO_EMAIL=nur.percin@hotmail.com
SMTP_HOST=smtp-mail.outlook.com
SMTP_PORT=587
SMTP_USER=nur.percin@hotmail.com
SMTP_PASSWORD=...   # Microsoft app password
SMTP_FROM=nur.percin@hotmail.com
```

Microsoft hesabında 2FA açıksa **App Password** oluşturun (normal şifre SMTP’de çalışmaz).

Gmail kullanacaksanız: `SMTP_HOST=smtp.gmail.com` + Gmail App Password.

## Kullanım

```bash
# Sadece tara + skorla, mail atma
python -m src.main --dry-run

# Gerçek mail gönder
python -m src.main

# Eşleşme olmasa da “bugün yeni yok” maili
python -m src.main --send-empty
```

veya:

```bash
chmod +x scripts/run_daily.sh
./scripts/run_daily.sh --dry-run
```

## Her gün otomatik çalıştırma

### Seçenek A — GitHub Actions (önerilen)

Repo’da `.github/workflows/daily-job-alerter.yml` her gün **07:00 UTC** çalışır.

GitHub → Settings → Secrets and variables → Actions içine ekleyin:

- `ALERT_TO_EMAIL`
- `SMTP_HOST` (örn. `smtp-mail.outlook.com`)
- `SMTP_PORT` (`587`)
- `SMTP_USER`
- `SMTP_PASSWORD` (Microsoft/Gmail **App Password**)
- `SMTP_FROM`
- `REED_API_KEY` (opsiyonel ama önerilir)
- `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` (opsiyonel ama önerilir)

Actions sekmesinden **Daily Job Alerter → Run workflow** ile hemen test edin.

### Seçenek B — Bilgisayarınızda cron

Linux/macOS crontab örneği (her gün 08:00):

```cron
0 8 * * * cd /path/to/job-alerter && /path/to/job-alerter/.venv/bin/python -m src.main >> data/cron.log 2>&1
```

Windows Task Scheduler ile aynı komutu günlük tetikleyebilirsiniz.

## Eşleştirme mantığı

Skor artışı:

- Hedef unvan benzerliği
- Hedef firma (Beko, Intertek, TÜV, Schneider, …)
- CV anahtarları (UKCA, CE, RoHS, REACH, IEC 60335, market access, …)

Elenebilir:

- AML / GDPR officer / pharmacovigilance gibi alakasız unvanlar
- “no longer accepting applications”, “job expired” vb. kapalı ilanlar
- Daha önce görülen / mail atılan ilanlar (SQLite: `data/jobs.sqlite3`)

Eşik: `MIN_MATCH_SCORE` (varsayılan 45).

## Profili özelleştirme

`config/profile.yaml`:

- `target_titles` — unvan listesi
- `target_companies` — firma listesi
- `search_queries` — board arama sorguları
- `cv_keywords` — skor ağırlıkları
- `exclude_title_keywords` — blacklist
- `sources` — hangi kaynaklar açık

## Test

```bash
python -m pytest tests/ -q
```

## Önemli sınırlar

- LinkedIn / Glassdoor agresif bot koruması kullanır; sonuçlar gün gün değişebilir.
- Resmi Reed + Adzuna API’leri en stabil kaynaktır — ikisini de bağlamanız önerilir.
- Bu araç **ilan bulur ve mail atar**; otomatik başvuru yapmaz (ToS / etik).
