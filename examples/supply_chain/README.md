# Analisis Rantai Pasok (Supply Chain Disruption Analysis) dengan AnchorDB

Studi kasus implementasi nyata AnchorDB untuk analisis dampak gangguan rantai pasok multi-tingkat (*multi-tier supply chain disruption*) menggunakan **Directed Causal Graph (Multi-Hop Traversal)** dan **In-Storage Arithmetic Computation**.

---

## 1. Domain Problem & Siapa yang Menggunakan

### Siapa yang Memakai Solusi Ini?
1. **Manufaktur Multinasional (OEM & Electronics Brands)**: Mengelola ribuan komponen dari ratusan supplier di berbagai negara (semikonduktor, modul kamera, baterai, sasis).
2. **Retail Besar & E-Commerce Global**: Memonitor ketersediaan stok produk akhir di jaringan pusat pemenuhan (*fulfillment centers*) dan toko flagship.
3. **Penyedia Layanan Logistik 3PL/4PL**: Mengoptimalkan rute kargo dan mengantisipasi efek domino keterlambatan pengiriman bahan baku terhadap pabrik perakitan.

---

## 2. Pertanyaan Bisnis Nyata

> ### *"Kalau pabrik semikonduktor di Bac Ninh, Vietnam terendam banjir akibat badai, produk (SKU) mana saja di toko ritel kita yang stoknya akan habis dalam 30 hari ke depan?"*

Pertanyaan ini terdengar sederhana, namun pada sistem database tradisional (Relational SQL atau Key-Value biasa) memerlukan kueri rekursif berulang (`JOIN` bertingkat 6 tabel) yang sangat lambat dan rawan *lock contention*, atau pemrosesan batch malam hari yang memakan waktu berjam-jam. 

AnchorDB menyelesaikan analisis ini dalam **beberapa mikrodetik** secara *in-memory* dan *zero-copy*.

---

## 3. Mengapa Butuh Multi-Hop (> 2 Hops)?

Gangguan di dunia nyata tidak berhenti di supplier tingkat pertama (Tier 1). Rantai pasok modern terdiri dari jaringan ketergantungan yang dalam:

```
[Hop 0: Bencana Alam]
   Pabrik Bac Ninh, Vietnam Terendam Banjir
               │
               ▼ (Hop 1: supplies_t1)
   Supplier Tier 1 (Fabrikasi Wafer SoC 3nm, Lensa Periskop, Display OLED)
               │
               ▼ (Hop 2: subassembles)
   Sub-Assembly Unit (Mainboard PCB, Modul Kamera OIS, Display Chassis)
               │
               ▼ (Hop 3: fabricates)
   Final Assembly Mega-Plant (Pabrik Perakitan Shenzhen, Penang, Chennai)
               │
               ▼ (Hop 4: transports)
   Regional Logistics Hub (Hub Kargo Singapura, Los Angeles, Rotterdam)
               │
               ▼ (Hop 5: distributes)
   Retail Megastores (Toko Flagship Tokyo, Seattle, Jakarta, Frankfurt, Sydney)
               │
               ▼ (Hop 6: stocks_sku)
   Final Product SKUs di Rak Toko (Smartphone Pro 1TB, Ultrabook Pro 16", Tablet Air 11", Smartwatch)
```

Jika query terhenti di **Hop 2**, pengambil keputusan hanya tahu bahwa *"Sub-assembly mainboard terganggu"*, tetapi **tidak tahu produk mana di toko ritel mana yang stoknya akan habis**. Analisis operasional membutuhkan visibilitas menyeluruh hingga **Hop 6**.

---

## 4. Mengapa Butuh Skor Probabilitas?

Ketergantungan rantai pasok bukan relasi biner (`true` atau `false`). 
- Komponen dengan **pemasok tunggal (*single source*)** memiliki koefisien transmisi kegagalan sangat tinggi ($0.95 - 0.98$). Jika pabrik hulu tutup, perakitan hilir pasti terhenti.
- Komponen dengan **pemasok ganda (*dual source*)** memiliki koefisien transmisi lebih rendah ($0.70 - 0.85$) karena pabrik perakitan dapat mengalihkan sebagian pesanan ke pabrik cadangan.
- Jalur logistik dengan cadangan buffer memiliki probabilitas redaman risiko.

AnchorDB menghitung probabilitas kumulatif di sepanjang jalur multi-hop secara otomatis melalui `CausalChain`:
$$\text{Probabilitas Transmisi Dampak} = \prod_{i=1}^{n} w_i$$

---

## 5. Mengapa Butuh In-Storage Math?

Mengetahui bahwa suatu produk terkena dampak gangguan probabilitas 79% baru setengah jawaban. Pertanyaan bisnisnya adalah: **"Apakah stoknya habis dalam 30 hari?"**

AnchorDB menyimpan atribut entitas:
- `inventory_units`: Jumlah unit buffer stok yang ada di gudang/toko saat ini.
- `daily_burn_rate`: Kecepatan konsumsi/penjualan rata-rata per hari.

AnchorDB mengevaluasi formula matematis langsung di dalam mesin penyimpanan (*in-storage computation*) tanpa mengekstrak data ke aplikasi:
$$\text{Days to Stockout} = \frac{\text{inventory\_units}}{\text{daily\_burn\_rate}}$$

Jika $\text{Days to Stockout} \le 30.0$, sistem secara otomatis membunyikan alarm **ALERT (<30 Hari)** dan memberikan rekomendasi tindakan mitigasi darurat (misal: pengalihan kargo udara ekspres atau pengalokasian stok cadangan regional).

---

## 6. Struktur File Proyek

```
examples/supply_chain/
├── main.go               # Program lengkap (Mode Embedded & Remote, Analisis 6-Hop, Crash-Test, Benchmark 5jt)
├── docker-compose.yml    # Konfigurasi container AnchorDB daemon dengan persistent volume & WAL
└── README.md             # Panduan operasional & dokumentasi arsitektur (file ini)
```

---

## 7. Cara Menjalankan

### Mode A: Embedded Mode (In-Process Runtime, Zero Dependency)
Mode ini menjalankan AnchorDB secara langsung di dalam proses Go tanpa memerlukan instalasi Docker, daemon eksternal, atau port jaringan TCP.

```bash
# Jalankan analisis skenario banjir Vietnam (6-Hop Causal + In-Storage Math)
go run ./examples/supply_chain/main.go -mode=embedded -scenario=analysis
```

#### Output Eksekutif yang Dihasilkan:
```text
>>> [PHASE 4] Executing Multi-Hop Causal Disruption Traversal (Hop 1 to Hop 6)
    ▶ Hop 1 (supplies_t1) -> Reached 4 downstream targets [Latency: 4.39µs]
    ▶ Hop 2 (supplies_t1 → subassembles) -> Reached 3 downstream targets [Latency: 2.79µs]
    ▶ Hop 3 (supplies_t1 → subassembles → fabricates) -> Reached 3 downstream targets [Latency: 3.04µs]
    ▶ Hop 4 (supplies_t1 → ... → transports) -> Reached 3 downstream targets [Latency: 11.33µs]
    ▶ Hop 5 (supplies_t1 → ... → distributes) -> Reached 5 downstream targets [Latency: 4.46µs]
    ▶ Hop 6 (supplies_t1 → ... → stocks_sku) -> Reached 6 downstream targets [Latency: 13.59µs]

┌───────────────────────────┬──────────────┬────────────┬──────────────┬─────────────┬──────────┬────────────────────────┐
│ Product SKU Name          │ Current Stock│ Burn Rate  │ Days to Empty│ Prob. Impact│ Status   │ Action Recommendation  │
├───────────────────────────┼──────────────┼────────────┼──────────────┼─────────────┼──────────┼────────────────────────┤
│ sku_ultrabook_pro_16in    │       150 u │     12.0/d │       12.5 d │       76.6% │ ALERT    │ EMERGENCY: Reroute air │
│ sku_smartphone_pro_1tb    │       240 u │     14.0/d │       17.1 d │       79.0% │ ALERT    │ EMERGENCY: Reroute air │
│ sku_smartwatch_fit_band   │       190 u │      9.0/d │       21.1 d │       71.1% │ ALERT    │ Allocate backup stock  │
│ sku_smartphone_base_128gb │       520 u │     20.0/d │       26.0 d │       75.0% │ ALERT    │ Allocate backup stock  │
│ sku_tablet_air_11in       │       480 u │     10.0/d │       48.0 d │       72.6% │ SAFE     │ Monitor replenishment  │
│ sku_smartwatch_ultra_49mm │       650 u │     11.0/d │       59.1 d │       70.3% │ SAFE     │ Monitor replenishment  │
└───────────────────────────┴──────────────┴────────────┴──────────────┴─────────────┴──────────┴────────────────────────┘
```

---

### Mode B: Container Mode (Docker & Podman)

Pada mode ini, AnchorDB berjalan sebagai daemon container server di port `7447`, dan aplikasi bertindak sebagai remote client (`driver.ConnectRemote`).

#### Langkah 1: Jalankan Container AnchorDB
Menggunakan **Docker Compose**:
```bash
cd examples/supply_chain
docker compose up -d
```

Atau menggunakan **Podman** secara langsung:
```bash
podman run -d \
  --name anchordb-supply-chain \
  -p 7447:7447 \
  -v ./data:/data:Z \
  -e ANCHORDB_PORT=7447 \
  -e ANCHORDB_DATA_DIR=/data \
  -e ANCHORDB_AUTH_TOKEN="supply_chain_secret_token" \
  -e ANCHORDB_WAL_SYNC=always \
  anchordb:latest
```

Verifikasi container berjalan normal:
```bash
curl -H "Authorization: Bearer supply_chain_secret_token" http://localhost:7447/health
```

#### Langkah 2: Jalankan Analisis Rantai Pasok via Remote Client
```bash
go run ./examples/supply_chain/main.go \
  -mode=remote \
  -url=http://localhost:7447 \
  -token=supply_chain_secret_token \
  -scenario=analysis
```

---

## 8. Skenario Crash Recovery & Uji Durabilitas

AnchorDB mengimplementasikan **Write-Ahead Logging (WAL)** berbasis frame binary dengan checksum CRC32 dan mode sinkronisasi disk fisik (`fsync`).

### Uji Otomatis (In-Process Simulated Crash):
```bash
go run ./examples/supply_chain/main.go -scenario=crash-test -phase=all
```

### Uji Nyata pada Container (Abrupt Container Kill & Reboot):

1. **Populasikan data awal & link 6-hop ke container**:
   ```bash
   go run ./examples/supply_chain/main.go \
     -mode=remote \
     -url=http://localhost:7447 \
     -token=supply_chain_secret_token \
     -scenario=crash-test \
     -phase=populate
   ```

2. **Simulasikan crash server tiba-tiba (Hard Kill)**:
   ```bash
   # Matikan container seketika tanpa graceful shutdown
   docker kill anchordb-supply-chain
   # (atau: podman kill anchordb-supply-chain)
   ```

3. **Hidupkan kembali container**:
   ```bash
   docker start anchordb-supply-chain
   # (atau: podman start anchordb-supply-chain)
   ```

4. **Verifikasi rekonsiliasi data dan graf**:
   ```bash
   go run ./examples/supply_chain/main.go \
     -mode=remote \
     -url=http://localhost:7447 \
     -token=supply_chain_secret_token \
     -scenario=crash-test \
     -phase=verify
   ```

Hasil verifikasi:
```text
[STEP 2/3] Verifying Data & Graph Integrity Post-Crash...
    [✓] Schema survived crash: 5 fields locked
    [✓] Canary record verified intact: inventory_units = 999999
    [✓] Causal graph verified: factory_vietnam_bac_ninh -> supplier_t1_soc_processor (Score: 0.9600)

==========================================================================================
    >>> DURABILITY VERIFICATION RESULT: 100% PASSED (0 DATA LOSS)
==========================================================================================
```

---

## 9. Uji Skala Besar (Dataset hingga 5 Juta Entitas & Relasi)

Untuk menguji performa AnchorDB pada skala enterprise besar (misal: 100.000 hingga 5.000.000 SKU / komponen rantai pasok global):

### Menjalankan Uji 100.000 Record:
```bash
go run ./examples/supply_chain/main.go -scenario=benchmark -scale=100000
```
- **Throughput Ingesti**: ~181.000 writes/detik
- **Throughput Pembuatan Relasi Graf**: ~141.000 edges/detik
- **Latensi Query Multi-Hop**: ~244 mikrodetik (**Sub-Milidetik!**)

### Menjalankan Uji Skala Penuh 5.000.000 Record (5jt):
```bash
go run ./examples/supply_chain/main.go -scenario=benchmark -scale=5000000
```

> **Catatan Penggunaan Memori**:
> Karena AnchorDB menggunakan arsitektur *memory-mapped file* (`mmap`), sistem tidak memuat seluruh 5 juta objek ke dalam Go Garbage Collector heap. Ingesti di-stream dalam batch multi-thread, sehingga footprint RAM tetap stabil dan bebas dari *GC pause*.
