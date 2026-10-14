# Filosofi Desain: Minimal Dependencies & Zero Bloat

> *"Jangan mengejar zero dependencies secara dogmatis, melainkan kejarlah minimal dependencies yang pragmatis, teruji, dan berkinerja tinggi."*

---

## 1. Latar Belakang: Mengapa Bukan "Zero Dependencies"?

Banyak proyek database bereksperimen dengan slogan **"Zero Dependencies"** (hanya memakai Go standard library). Namun, dalam rekayasa perangkat lunak produksi tingkat lanjut, pendekatan *zero dependencies* yang dogmatis sering kali menimbulkan efek samping negatif:
1. **Reinventing the Wheel**: Menulis ulang algoritma kompleks secara mandiri (misalnya enkripsi, kompresi, background scheduling, atau antrean terdistribusi) sering kali menghasilkan implementasi yang rawan *bug*, kurang optimal, dan sulit diaudit.
2. **Kerapuhan Pemeliharaan**: Kode internal membengkak dengan fungsi utilitas *boilerplate* yang sebenarnya sudah diuji oleh jutaan pengembang di pustaka (*libraries*) standar industri.
3. **Kompromi Performa**: Menolak pustaka khusus (seperti SIMD vector primitives, panic recovery, atau high-performance message routing) demi label "zero dependencies" mengorbankan potensi performa dan stabilitas nyata.

Oleh karena itu, **AnchorDB menetapkan filosofi: Minimal Dependencies (Minimalis, Terkurasi, dan Bebas Bloat)**.

---

## 2. Kriteria Ketat Pemilihan Dependensi

Setiap dependensi yang masuk ke dalam AnchorDB harus memenuhi 5 aturan emas (*The 5 Golden Rules*):

| # | Aturan | Penjelasan |
|---|---|---|
| **1** | **Zero CGO** | Dependensi harus 100% pure Go. Tidak boleh ada dependensi C/C++ yang membutuhkan toolchain CGO, glibc version locking, atau kompilasi silang yang rumit. |
| **2** | **No Transitive Tree Explosion** | Dependensi yang membawa puluhan sub-dependensi lain (dependency graph raksasa) **dilarang keras**. Dependensi yang diterima adalah yang berdiri sendiri (*self-contained* atau zero transitive dependencies). |
| **3** | **Zero External Daemons** | Engine AnchorDB harus berdiri sendiri secara mandiri tanpa mewajibkan broker eksternal (tidak bergantung pada Redis, NATS, Kafka, atau RabbitMQ). |
| **4** | **Production-Grade & Battle-Tested** | Pustaka harus aktif dipelihara, memiliki cakupan pengujian (*test coverage*) tinggi, dan terbukti stabil di sistem berbeban tinggi. |
| **5** | **Tiny Binary Impact** | Binary final AnchorDB harus tetap ringkas (< 15 MB) dan waktu kompilasi `go build` harus tetap berada di bawah 2 detik. |

---

## 3. Studi Kasus Implementasi: Background Pipeline (`gosuite/v3`)

Sebagai wujud nyata filosofi *Minimal Dependencies*, AnchorDB mengintegrasikan subsistem latar belakang dari `github.com/rachmanzz/gosuite/v3`:

1. **`cron` (`v3/cron`)**:
   - Menghadirkan penjadwal tingkat produksi dengan proteksi *skip-if-still-running*, panic recovery, dan graceful shutdown berbatas waktu.
   - Mengotomatiskan partisi harian dan pemicu sync berkala ke disk.
2. **`worker` (`v3/worker`)**:
   - Menjalankan **Async Partitioning** (`POST /v1/partition/run?async=true`) sehingga request HTTP langsung dijawab dalam hitungan milidetik, sementara kompresi berkas dan upload S3 diselesaikan oleh worker di latar belakang.
   - Menjalankan **Periodic Background Disk Sync** (`db.Sync()`) secara kontinu tanpa mengunci query pembacaan data.

---

## 4. Keuntungan Nyata untuk Pengguna

1. **Keamanan Maksimal**: Audit keamanan menjadi mudah karena *surface attack* kecil dan kode mudah diperiksa secara menyeluruh.
2. **Kompilasi Cepat**: `go build` berlangsung instan dalam hitungan detik, baik di mesin lokal pengembang maupun di pipeline CI/CD.
3. **Container Super Ringan**: Image Docker/Podman berbasis Alpine tetap berukuran sangat ramping (**~14–15 MB**).
4. **Kemudahan Operasional**: Cukup 1 binary atau 1 container independen tanpa perlu memasang cluster Redis atau broker eksternal lainnya.
