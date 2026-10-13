# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Pham Minh Hieu |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602919` |
| GitHub | BenKei-19 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-08888` |

> Về trip: MSSV có chữ `A` nên `int(MSSV) % 26` trong GUIDE không chạy được. Mình bỏ ký tự không phải số: `2202602919 % 26 = 23`, và `list-trips[23] = trip-08888`.

## Kết quả cuối

- `dataset_version` của bạn: `971be7e3a807`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 678 / 147 / 194 (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png` (run `crash_1` xanh sau khi clear), `minio_lake.png`, `metabase_dashboard.png` (có 2 câu hỏi riêng ở cuối), `airflow_crash_log_trip-08888.png` (log attempt 1 đỏ: `simulated crash while extracting trip-08888`), `airflow_rerun_attempt2_success.png` (attempt 2 xanh: `{"trip": "trip-08888", "frames": 60}`).
- Đầu ra thô của từng bước nằm trong `report/runs/` (`step1.txt` … `step5_crash_rerun.txt`, `final_step_all.txt`).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt mình thấy ở `raw_telemetry_a` (3021 dòng) và `raw_telemetry_b` (3234 dòng):
1. **Tên cột**: `ts_utc / speed_mps / heading_deg / lat / lon / acc_x / gas / brake / steer` (A) so với `timestamp_ms / spd_kmh / hdg / latitude / longitude / ax / throttle_pct / brake_pct / steering` (B).
2. **Đơn vị tốc độ**: A dùng m/s (`1.213`), B dùng km/h (`55.11`).
3. **Kiểu timestamp**: A là chuỗi ISO-8601 UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Bàn đạp**: A là tỷ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: `bronze_video_files` ghi `telemetry_clock_offset_ms = 1500` cho drive_b và 0 cho drive_a. Đồng hồ thiết bị B chạy nhanh hơn video 1,5 giây.

Bronze là bản ghi gốc, chỉ thêm. Nếu gộp và ép kiểu ngay lúc ingest thì: ô trống (30 dòng thiếu tốc độ mỗi drive) hoặc giá trị hỏng làm hỏng lệnh cast hoặc bị biến thành NULL im lặng; ta mất dữ liệu gốc để đối chiếu; và khi luật chuyển đổi đổi (ví dụ độ lệch đồng hồ khai sai) thì không xây lại silver được. Để mọi cột ở dạng chữ, mỗi nguồn một bảng, thì schema drift của B không làm hỏng A, và mọi luật nằm ở một chỗ duy nhất là staging, nơi ta sửa và chạy lại được. Nạp lại lần hai báo `already present` (nhận diện bằng sha256), và `history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema của A, đổi `spd_kmh / 3.6` sang m/s, đổi pedal `% / 100` về 0..1, và chuyển thời gian bằng `to_timestamp((timestamp_ms - offset_ms) / 1000)`, trong đó `offset_ms = 1500` lấy từ `bronze.video_files` (vốn đến từ `sources.json`). Bằng chứng: cột `bat_dau` của B là `2025-10-08 14:30:00.009`, trùng thời điểm bắt đầu video `14:30:00` (lệch 9 ms, check "within 0.5 s" PASS). Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), đều hợp lý. Nếu quên chia 3.6 thì B sẽ ra khoảng 360 km/h.

| drive | duplicate | speed_missing | speed_out_of_range | sạch + cách ly = nhận |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 + 53 = 3234 |

Dòng lỗi vào `silver_telemetry_rejects` (106 dòng) kèm `reject_reason`, không bị xoá, vì: (1) đối soát được, sạch + cách ly luôn bằng số dòng nhận, nên không dòng nào biến mất không lý do; (2) khi luật sai (ví dụ ngưỡng 70 m/s quá chặt) thì ta sửa luật rồi xử lý lại, không cần ingest lại; (3) số dòng lỗi theo lý do là tín hiệu giám sát cảm biến/nguồn (dashboard "Telemetry quarantine by reason").

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng hở > 1000 ms giữa hai mẫu) là lỗi **đã biết và chấp nhận được**: cảm biến có lúc mất tín hiệu thật, dữ liệu vẫn đúng, chỉ thiếu. Dừng cả pipeline vì nó thì không bao giờ ra được dataset. Hậu quả của nó đã được xử lý ở chỗ khác: khung hình rơi vào khoảng hở bị loại vì `not sync_ok` (7 + 7 khung, độ trễ tối đa 2884 / 3112 ms). Ngược lại, `in_range` (0..70 m/s) mà sai nghĩa là có **giá trị không thể xảy ra** lọt vào silver. Đó là dữ liệu hỏng, sẽ thành nhãn sai cho mô hình, nên phải dừng (`error`).

Thí nghiệm: nới `max_speed_mps` từ 70 lên 1000 thì 6 dòng vô lý (3 + 3, chính là 6 dòng `speed_out_of_range`) lọt vào silver. `dbt test` ra `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**, `gate_failed: true`. Khôi phục thì về `PASS=6 WARN=1 ERROR=0`. Ý nghĩa: cổng chất lượng là lớp kiểm tra **độc lập** với luật làm sạch. Luật làm sạch có thể bị sửa sai hoặc có bug, nhưng test ở đầu ra vẫn bắt được và chặn các task gold/warehouse phía sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại partition `trip-00003` (60 khung) hai lần: trước **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau hai lần chạy lại vẫn **1246 dòng**, `6c7480dfde7e` (`unchanged: true`). `history silver.frames` thấy hai commit mới (version 26, 27) đều là `WRITE` với `predicate trip_id = 'trip-00003'`, `rows_added 60`: mỗi lần **thay** đúng 60 dòng của trip đó. Cách ghi `append` sai, chạy 2 lần, cho **120 dòng** cho một trip 60 khung, tức **nhân đôi**.

Delta `overwrite` với predicate là **một commit nguyên tử**: việc gỡ file cũ của trip và thêm file mới nằm trong cùng một mục của transaction log. Người đọc chỉ thấy phiên bản cũ hoặc mới, không bao giờ thấy trạng thái nửa vời. Nếu lỗi xảy ra giữa chừng thì không có commit, bảng giữ nguyên phiên bản trước. Mình thấy đúng điều này ở bài tập sập: ngay sau khi trip-08888 sập, `silver.frames` có 1186 dòng, trip-08888 = 0 dòng, không có dòng rác. Xoá rồi ghi lại là **hai** thao tác: sập ở giữa thì partition mất trắng hoặc ghi dở, người đọc đồng thời thấy "lỗ", và muốn sửa phải tự dọn. Ngoài ra Delta còn giữ phiên bản cũ để time travel nếu ghi nhầm.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** có độ sáng trung bình < 12 (`dark_luma`): không có thông tin (camera hỏng, mất tín hiệu), nên **bị loại** (A 6, B 4). Khung **tối** có độ sáng < 40 (`dark_soft`) nhưng không đen: đường hầm, bóng râm, đều là điều kiện lái thật, nên **gắn cờ `is_dark` nhưng giữ lại** (A 4, B 61; verify "low-light flagged but not black" 10/10). Mình đồng ý: mô hình chạy ngoài đời phải gặp đường hầm, bỏ khung tối thì dataset bị lệch về cảnh sáng. Cờ vẫn giúp lọc hoặc đánh giá riêng nhóm này khi cần.

Mình chọn **ngưỡng 2** (đang dùng). Ngưỡng 0 chỉ bắt khung giống hệt bit, nên bỏ sót camera đứng hình, vì nhiễu nén làm vài bit khác nhau (ở ngưỡng 2 bắt được 11/12 khung đứng hình). Ngưỡng 6 coi 363/602 = 60% khung của A và 244/644 = 38% của B là trùng, tức loại cả những khung khác nhau thật. Ngưỡng phụ thuộc tốc độ: lấy mẫu 2 khung/giây theo **thời gian**, nên xe chậm (A: tốc độ train trung bình 29 km/h, có đoạn đứng hình) có hai khung liền nhau gần như giống hệt. Vì vậy A có 149 khung trùng so với 53 của B (71 km/h). Tốt hơn nữa là lấy mẫu theo **quãng đường** (mỗi N mét) hoặc cho ngưỡng thay đổi theo `speed_mps`.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trên 1019 khung đáng dùng thuộc 26 trip: chia **ngẫu nhiên theo khung hình** (70/15/15) làm **26/26 trip** nằm ở nhiều hơn một split, và cả **26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0** trip bị chia cắt, và test `no_straddle` (mức `error`) canh điều này. Hai khung liền nhau chỉ cách 0,5 giây, cùng con đường, cùng xe phía trước, cùng ánh sáng. Ngay cả sau khi bỏ khung trùng ở ngưỡng 2, chúng vẫn rất giống nhau. Nếu nửa trip ở train và nửa ở test thì mô hình chỉ cần **nhớ cảnh** là làm tốt bài test. Điểm test khi đó đo khả năng nhớ, không đo khả năng tổng quát hoá sang con đường **mới**, trong khi ngoài đời xe luôn gặp chuyến mới. Kết quả là điểm test cao giả tạo và mô hình thất vọng khi triển khai.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` đã sắp xếp (`pipeline/publish.py:69`). Vì là băm nội dung nên **cùng nội dung thì cùng tên, khác nội dung thì khác tên**, không phụ thuộc giờ chạy hay máy chạy. Ngày giờ hay số đếm sẽ cho hai tên khác nhau cho cùng một dataset, nên không biết hai lần huấn luyện có dùng cùng dữ liệu không; hoặc ngược lại, hai máy cùng ra "v3" cho hai nội dung khác nhau.

Mình chạy 5 lần (step all đầu tiên, step 4, `cli export-dataset` chạy lại, sau bài tập sập/chạy lại, step all cuối) và **cả 5 lần đều ra `971be7e3a807`**, cùng `manifest_sha256 937d2a87…`. MSSV được trộn vào thứ tự xếp trip: `row_number() over (partition by drive_id order by md5(trip_id || LAB_STUDENT_ID))` trong `gold_dataset_frames.sql`. Thứ tự đó quyết định trip nào vào train/val/test, nên cột `split` đổi và băm đổi. Mình thử `LAB_STUDENT_ID=2024000` thì ra `8fc131884584` với train/val/test = 703/182/134 (của mình là 678/147/194). Tổng khung vẫn 1019 và vẫn 18/4/4 trip. Đặt lại `2A202602919` thì về đúng `971be7e3a807` (`report/runs/step4_doi_mssv.txt`).

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/971be7e3a807/manifest.json` ghi rằng dataset 1019 khung này được tạo từ: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.frames` **v25** (26 lần ghi partition, v0..v25), `silver.telemetry` v0, `silver.frames_flagged` v0, `silver.frame_telemetry` v0; và hai video gốc có sha256 `636ba1a9…e7e9aa` (drive_a) và `bfa62703…ee9fdc` (drive_b). Luật: bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; chia "per trip, stratified by drive, md5 order".

3 tháng sau, để tái tạo: (1) sha256 video gốc để tìm đúng file; (2) số version của từng bảng Delta để time travel đọc đúng trạng thái; (3) `dataset_version` và `manifest_sha256` để so kết quả dựng lại có giống bit không. Manifest **còn thiếu** hai thứ mình phải tự giữ: MSSV (muối của split) và các ngưỡng trong `dbt_project.yml` (`dup_hamming=2`, `sync_tol_ms=150`, …) hay commit git của code. Đổi các ngưỡng này thì dataset đổi (xem thí nghiệm riêng bên dưới), nhưng manifest không ghi lại.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với `LAB_FAIL_ONCE=trip-08888`, sau `day09_reset` rồi `day09_video_pipeline` (run `crash_1`, 37 task instance): **1 task đỏ** là `silver_frames` map_index 23. Log attempt 1 ghi `RuntimeError: simulated crash while extracting trip-08888; run it again`. Có **7 task `upstream_failed`** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) và 29 task success: 25 trip khác (kể cả map 24, 25 chạy sau trip hỏng), 3 task bronze và `trip_commands`. Ngay sau khi sập, `silver.frames` có 1186 = 1246 − 60 dòng, trip-08888 = 0 dòng: không có dữ liệu ghi dở.

Mình chỉ clear task đỏ cùng các task downstream đang `upstream_failed` (`airflow tasks clear -t ^silver_frames$ --only-failed --downstream`, tương đương nút Clear trên giao diện). Có **8 task chạy lại**, chỉ map_index 23 có `attempt=2.log` (27 file log cho 26 trip). 25 trip đã xong **không** chạy lại. DAG chuyển xanh (37/37 success). Sau đó `silver.frames` có 1246 dòng, trip-08888 = 60, và 1246 `frame_id` khác nhau: **không nhân đôi**. `verify` cho 27/27, `dataset_version` **vẫn `971be7e3a807`**. Bằng chứng: `report/runs/step5_crash_rerun.txt`, `report/runs/crash_trip-08888_attempt1.log`, và ảnh `report/screenshots/`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ước lượng thô: hai đoạn 5 phút ở 640×360 đã là 27 MB. Một xe chạy 8 giờ một camera là khoảng 1,3 GB, nên 500 xe là khoảng 650 GB/ngày cho **một** camera độ phân giải thấp; đủ 7 camera gốc thì lên hàng chục TB/ngày.
1. **Decode video (`silver_frames`)**: FFmpeg CPU trong một container Airflow, `LocalExecutor`, `PARALLELISM=2`, `max_active_tis_per_dagrun=1` (các trip chạy **tuần tự**, mỗi trip khoảng 2 s). Đây là nút cổ chai vỡ đầu tiên. Thay bằng một cụm worker co giãn (Kubernetes/Celery executor hoặc Spark/Ray) với decode GPU (NVDEC), và lấy mẫu ngay khi ingest hoặc trên xe để không decode lại nhiều lần.
2. **DuckDB một file + dbt trên một máy** (`lakehouse.duckdb`): không chia sẻ được giữa nhiều worker, bị giới hạn RAM. Thay bằng engine phân tán đọc thẳng bảng Delta (Spark/Databricks, Trino), model dbt **incremental** theo ngày/xe thay vì dựng lại toàn bảng.
3. **Điều phối kiểu "một task mỗi trip, một DAG cho tất cả"**: 500 xe × vài chục trip là hàng chục nghìn mapped task mỗi ngày, đè nặng scheduler và metadata DB. Thêm nữa, `day09_reset` xoá cả lake, không dùng được ở quy mô này. Thay bằng ingest hướng sự kiện (thông báo S3/Kafka khi xe upload), partition theo `ngày/xe`, gom nhiều trip vào một task, backfill theo partition. Storage cũng phải chuyển từ MinIO một node sang S3 hoặc MinIO phân tán, có vòng đời dữ liệu (video thô chuyển sang lớp lưu trữ lạnh).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Card "Câu hỏi riêng: tỷ lệ khung bị loại theo trip":

```sql
select trip_id, drive_id, avg_speed_kmh, frames, frames_kept, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as drop_pct, split
from marts.mart_trip_summary order by drop_pct desc, trip_id
```

Kết quả đứng đầu: trip-04622 loại **56,7%** (34/60), trip-03904 50,0% (11/22), trip-02086 46,7% (28/60), trip-08888 40,0% (24/60, chạy 6,5 km/h), trip-07774 38,2%. Cả 5 trip đều thuộc **drive_a**. Tra trong `silver.silver_frame_telemetry`, **toàn bộ** số khung bị loại của 5 trip này là `is_near_dup`; không có khung đen, mờ hay lệch đồng bộ. Nghĩa là thứ "ăn" dataset nhiều nhất là luật khung trùng, không phải lỗi camera. Nó cũng làm lệch split: trip-04622 và trip-02086 nằm ở val, nên val của drive_a chỉ còn 58 khung.

Card thứ hai (điểm cộng), "tỷ lệ loại theo dải tốc độ của trip":

```sql
select case when avg_speed_kmh < 20 then '0-20' when avg_speed_kmh < 50 then '20-50' else '50+' end as speed_band,
       count(*) as trips, round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as drop_pct
from marts.mart_trip_summary group by 1 order by 1
```

Kết quả: 0–20 km/h loại **32,4%** (5 trip), 20–50 km/h loại 17,1% (7 trip), 50+ km/h loại 14,4% (14 trip). Xe chậm thì nhiều khung trùng, khớp với phân tích ở Câu 5. Dataset vì thế ít khung ở tốc độ thấp (đô thị, kẹt xe) hơn tỷ lệ thời gian xe thực sự chạy ở tốc độ đó.

## Thí nghiệm riêng (điểm cộng): `sync_tol_ms`

Chạy lại `dbt run --select silver_frame_telemetry+ --vars '{sync_tol_ms: X}'` (không sửa file; lần chạy cuối `step all --reset` dựng lại với mặc định 150). Kết quả trong `report/runs/exp_rieng_sync_tol.txt`:

| `sync_tol_ms` | khung lệch đồng bộ A / B | gold train / val / test | tổng gold |
|---|---|---|---|
| 50 | 272 / 328 | 341 / 89 / 108 | 538 |
| **150** (mặc định) | 7 / 7 | 678 / 147 / 194 | 1019 |
| 300 | 7 / 7 | 678 / 147 / 194 | 1019 |

Ở 50 ms, gold **mất 47% khung**. Telemetry khoảng 13 mẫu/giây (các mẫu cách nhau khoảng 77–80 ms, xem 2 dòng đầu ở bước 1), nên mẫu gần nhất thường cách khung hình hơn 50 ms dù dữ liệu không có lỗi gì. Từ 150 lên 300 ms không đổi gì: 14 khung còn lại là các đoạn cảm biến mất tín hiệu thật (trễ tới 2,9–3,1 s), nên ngưỡng nào hợp lý cũng loại chúng. Vậy 150 ms nằm ở vùng ổn định, khoảng gấp đôi chu kỳ lấy mẫu telemetry. Ngưỡng này phải đặt theo tần số cảm biến, không phải theo cảm tính.

## Nhìn lại

Điều bất ngờ nhất: sập giữa chừng rồi clear một task mà **không cần dọn gì**. Delta không commit thì bảng như chưa có gì xảy ra, và `dataset_version` không đổi qua 5 lần chạy. Điều mình chưa rõ: trip-04622 chạy **67 km/h** mà vẫn có 34/60 khung "trùng". Mình đoán dHash 64 bit quá thô với cảnh cao tốc ít chi tiết (trời, mặt đường), hoặc đoạn camera đứng hình 6 giây rơi vào trip này. Mình chưa kiểm chứng bằng cách xem ảnh. Lỗi dữ liệu mình nghĩ pipeline **chưa** bắt: (1) khung "trùng giả" như trên bị loại oan, vì không có test nào kiểm tra tỷ lệ near-dup theo tốc độ; (2) telemetry nằm trong 0..70 m/s nhưng nhảy vô lý giữa hai mẫu liền nhau (gia tốc không khớp `acc_x`), hoặc GPS nhảy, vẫn qua được `in_range`; (3) độ lệch đồng hồ chỉ tin vào giá trị **khai báo** 1500 ms; nếu khai sai thì không test nào phát hiện. Ngoài ra, khi chạy trên Windows, entrypoint Airflow lỗi do git đổi `.sh` sang CRLF; mình sửa bằng `.gitattributes` (`*.sh eol=lf`).
