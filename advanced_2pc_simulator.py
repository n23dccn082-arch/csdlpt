import time
import random
import uuid
import pandas as pd
import matplotlib.pyplot as plt
import os
from concurrent.futures import ThreadPoolExecutor

class SensorReading:
    def __init__(self, sensor_id, timestamp, temperature, humidity, region):
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity
        self.region = region

    def to_dict(self):
        return {
            "SensorID": self.sensor_id,
            "Timestamp": self.timestamp,
            "Temperature": self.temperature,
            "Humidity": self.humidity,
            "Region": self.region
        }

import threading

class ParticipantNode:
    def __init__(self, name, db_fail_rate=0.03):
        self.name = name
        self.storage_file = f"{name}_storage.csv"
        self.db_fail_rate = db_fail_rate # Tỷ lệ Node tự Vote Abort do lỗi DB nội bộ
        self.lock_table = {}
        self.transaction_states = {} # tx_id -> state ('INIT', 'READY', 'COMMITTED', 'ABORTED')
        self.prepare_times = {} # tx_id -> timestamp (ms)
        self.lease_durations = {} # tx_id -> duration (ms)
        
        # Bộ ghi nhận thời gian chiếm giữ khóa thực tế trên đĩa vật lý của Site
        self.lock_acquire_times = {} # tx_id -> timestamp (ms)
        self.lock_hold_durations = {} # tx_id -> hold_time (ms)
        
        #  Khóa tương trợ luồng để tránh Race Condition giữa ThreadPoolCoordinator và Tiến trình Expiry ngầm
        self.lock = threading.Lock()
        
        # Tạo tiến trình ngầm chủ động quét và hủy khóa hết hạn độc lập (Proactive Expiry Thread Daemon)
        self.bg_thread = threading.Thread(target=self._proactive_lease_checker, daemon=True)
        self.bg_thread.start()
        
    def _proactive_lease_checker(self):
        while True:
            time.sleep(0.005) # Quét tần suất 5ms để phát hiện hết hạn cực nhạy
            with self.lock:
                # Quét tất cả các giao dịch đang ở trạng thái READY để xem có cái nào hết hạn không
                active_txs = [tx for tx, state in self.transaction_states.items() if state == 'READY']
                for tx_id in active_txs:
                    self._check_lease_expiry_under_lock(tx_id)
        
    def prepare(self, tx_id, batch_data, lease_duration, network_delay_range):
        # Giả lập trễ mạng chiều đi (gửi Prepare từ Coordinator tới Site)
        delay = random.uniform(*network_delay_range)
        time.sleep(delay / 1000.0)
        
        with self.lock:
            print(f"[{self.name}] Received PREPARE for TX {tx_id} with Lease {lease_duration}ms")
            self.transaction_states[tx_id] = 'READY'
            self.prepare_times[tx_id] = time.time() * 1000
            
            #  Sai lệch mốc tính thời gian Lease (Clock Drift/Delay Drift)
            # Trừ hao chính xác độ trễ mạng 'delay' từ thời điểm gửi đến thời điểm nhận để đồng bộ với bộ đếm Coordinator
            self.lease_durations[tx_id] = max(0.0, lease_duration - delay)
            
            # Giả lập lỗi ghi cơ sở dữ liệu nội bộ (DB Failure) dẫn tới Vote Abort
            if random.random() < self.db_fail_rate:
                print(f"[{self.name}] internal DB error! Voting VOTE_ABORT.")
                self._abort_under_lock(tx_id)
                return "VOTE_ABORT"
                
            # Thu hồi ổ khóa khóa tài nguyên (Sensor ID) và lưu trữ thời gian lấy khóa thực tế
            self.lock_acquire_times[tx_id] = time.time() * 1000
            for record in batch_data:
                self.lock_table[record['SensorID']] = tx_id
                
            return "VOTE_COMMIT"

    def commit(self, tx_id, batch_data, network_delay_range):
        # Giả lập trễ mạng chiều đi (gửi Quyết định Commit)
        delay = random.uniform(*network_delay_range)
        time.sleep(delay / 1000.0)
        
        with self.lock:
            self._check_lease_expiry_under_lock(tx_id)
            if self.transaction_states.get(tx_id) == 'ABORTED':
                print(f"[{self.name}] Late COMMIT rejected for TX {tx_id} (Already Aborted due to Lease Expiry)")
                return "FAIL_ALREADY_ABORTED"
                
            print(f"[{self.name}] Received COMMIT for TX {tx_id}")
            if self.transaction_states.get(tx_id) == 'READY':
                # Ghi dữ liệu vật lý vào file CSV cục bộ
                df = pd.DataFrame(batch_data)
                if not os.path.exists(self.storage_file):
                    df.to_csv(self.storage_file, index=False)
                else:
                    df.to_csv(self.storage_file, mode='a', header=False, index=False)
                    
                self.transaction_states[tx_id] = 'COMMITTED'
                # Đo lường thời gian chiếm giữ khóa thực tế từ prepare tới commit
                if tx_id in self.lock_acquire_times:
                    self.lock_hold_durations[tx_id] = (time.time() * 1000) - self.lock_acquire_times[tx_id]
                
                self._release_locks(tx_id)
                return "ACK_COMMIT"
            return "FAIL"

    def abort(self, tx_id, network_delay_range=None):
        if network_delay_range:
            delay = random.uniform(*network_delay_range)
            time.sleep(delay / 1000.0)
            
        with self.lock:
            self._abort_under_lock(tx_id) # Gọi hàm nội bộ hủy bỏ dưới khóa lock
            return "ACK_ABORT"

    def _abort_under_lock(self, tx_id):
        if self.transaction_states.get(tx_id) == 'ABORTED':
            return
        print(f"[{self.name}] Aborting TX {tx_id}")
        self.transaction_states[tx_id] = 'ABORTED'
        
        # Đo lường thời gian chiếm giữ khóa thực tế khi bị Abort
        if tx_id in self.lock_acquire_times:
            self.lock_hold_durations[tx_id] = (time.time() * 1000) - self.lock_acquire_times[tx_id]
            
        self._release_locks(tx_id)# Mở khóa Lock Table

    def check_lease_expiry(self, tx_id):
        with self.lock:
            self._check_lease_expiry_under_lock(tx_id)

    def _check_lease_expiry_under_lock(self, tx_id):
        if self.transaction_states.get(tx_id) == 'READY':
            elapsed = (time.time() * 1000) - self.prepare_times[tx_id] # Tính thời gian trôi qua thực tế
            if elapsed > self.lease_durations[tx_id]:
                print(f"[{self.name}] LEASE EXPIRED for TX {tx_id} ({elapsed:.1f}ms > {self.lease_durations[tx_id]:.1f}ms). Local Auto-Abort triggered!")
                self._abort_under_lock(tx_id)

    def _release_locks(self, tx_id):
        keys_to_release = [k for k, v in self.lock_table.items() if v == tx_id]
        for k in keys_to_release:
            del self.lock_table[k]

class Coordinator:
    def __init__(self, nodes):
        self.nodes = nodes # Danh sách 3 Site cục bộ (North, Central, South)
        
    def execute_transaction(self, tx_id, batch_data, lease_duration, network_delay_range, failure_scenario=None):
        # Route dữ liệu (Phân mảnh ngang theo Region)
        fragmented_data = {region: [] for region in self.nodes.keys()}
        for record in batch_data:
            region = record['Region']
            if region in fragmented_data:
                fragmented_data[region].append(record)

        #   2. PHA 1: GỬI PREPARE ĐỒNG THỜI QUA ĐA LUỒNG (ThreadPoolExecutor)
        votes = {}
        def dispatch_prepare(region):
            data = fragmented_data[region]
            if not data:
                return region, "NO_DATA"
            node = self.nodes[region]
 # Gọi hàm prepare cục bộ của từng Site

            vote = node.prepare(tx_id, data, lease_duration, network_delay_range)
            return region, vote

        active_regions = [r for r, d in fragmented_data.items() if d]
        # Bắn Parallel RPC đồng thời tới các chi nhánh
        
        with ThreadPoolExecutor(max_workers=len(active_regions)) as executor:
            prepare_results = list(executor.map(dispatch_prepare, active_regions))
            
        for region, vote in prepare_results:
            if vote != "NO_DATA":
                votes[self.nodes[region].name] = vote

# QUYẾT ĐỊNH DỰA TRÊN PHIẾU BẦU

        all_commit = all(v == "VOTE_COMMIT" for v in votes.values()) and len(votes) > 0
        decision = "GLOBAL_COMMIT" if all_commit else "GLOBAL_ABORT"
        # GIẢ LẬP SỰ CỐ: COORDINATOR CRASH SAU PHA PREPARE
        if failure_scenario == 'coordinator_crash_after_prepare':
            print("\n[SYSTEM ALERT] Coordinator crashed after PREPARE phase! No decision sent to participants.")
            return "COORDINATOR_CRASHED"

  # 3. PHA 2: GỬI QUYẾT ĐỊNH ĐỒNG THỜI QUA ĐA LUỒNG (COMMIT / ABORT)
        results = {}
        def dispatch_decision(region):
            data = fragmented_data[region]
            if not data:
                return region, "NO_DATA"
            node = self.nodes[region]
            node.check_lease_expiry(tx_id)
            
            if node.transaction_states.get(tx_id) == 'ABORTED':
                return region, "FAIL_ALREADY_ABORTED"
                
            if decision == "GLOBAL_COMMIT":
                ack = node.commit(tx_id, data, network_delay_range)
                return region, ack
            else:
                ack = node.abort(tx_id, network_delay_range)
                return region, ack

        with ThreadPoolExecutor(max_workers=len(active_regions)) as executor:
            decision_results = list(executor.map(dispatch_decision, active_regions))
            
        for region, ack in decision_results:
            if ack != "NO_DATA":
                results[self.nodes[region].name] = ack
                    
        return "COMMITTED" if all(r == "ACK_COMMIT" for r in results.values()) else "ABORTED"

def generate_and_save_dataset(filename="sensor_readings.csv", count=500):
    print(f"Generating physical dataset: {filename} with {count} records...")
    regions = ['Site_North', 'Site_Central', 'Site_South']
    data = []
    base_time = pd.Timestamp.now()
    for i in range(count):
        r = random.choice(regions)
        reading = SensorReading(
            sensor_id=f"SEN-{random.randint(100, 199)}",
            timestamp=(base_time + pd.Timedelta(seconds=i)).isoformat(),
            temperature=round(random.uniform(20.0, 35.0), 2),
            humidity=round(random.uniform(40.0, 80.0), 2),
            region=r
        )
        data.append(reading.to_dict())
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Dataset generated and saved successfully to {filename}!")
#runnnn

def run_experiment():
    dataset_file = "sensor_readings.csv"
    # Tự động tạo lại nếu tệp chưa tồn tại hoặc số dòng dữ liệu khác 10000 (đã nâng cấp)
    if not os.path.exists(dataset_file) or (os.path.exists(dataset_file) and len(pd.read_csv(dataset_file)) != 10000):
        if os.path.exists(dataset_file):
            os.remove(dataset_file)
        generate_and_save_dataset(dataset_file, 10000)
        
    df_dataset = pd.read_csv(dataset_file)
    
    # Dọn dẹp lưu trữ cũ
    for name in ['Site_North', 'Site_Central', 'Site_South']:
        f = f"{name}_storage.csv"
        if os.path.exists(f):
            os.remove(f)
            
    print("\nInitializing distributed nodes for IoT Sensor Data...")
    nodes = {
        'Site_North': ParticipantNode('Site_North', db_fail_rate=0.03),
        'Site_Central': ParticipantNode('Site_Central', db_fail_rate=0.03),
        'Site_South': ParticipantNode('Site_South', db_fail_rate=0.03)
    }
    coordinator = Coordinator(nodes)
    
    lease_values = [20, 40, 60, 80, 100, 120, 150, 200, 300]
    num_tests = 30
    network_delay_range = (10, 50) # Mạng trễ 1 chiều: 10ms - 50ms (RTT: 20ms - 100ms)
    
    results = []
    print("\nStarting experimental runs for Lease Optimization...")
    for lease in lease_values:
        commits = 0
        aborts = 0
        blocking_times = []
        
        for i in range(num_tests):
            tx_id = f"TX-{lease}-{i}"
            start_idx = (i * 5) % len(df_dataset)
            batch = df_dataset.iloc[start_idx : start_idx + 5].to_dict('records')
            
            status = coordinator.execute_transaction(tx_id, batch, lease, network_delay_range)
            
            if status == "COMMITTED":
                commits += 1
            else:
                aborts += 1
                
            # Đo lường thời gian chiếm giữ khóa thực tế (Actual lock hold time)
            # Truy vấn trực tiếp từ các Site tham gia xem thời gian giữ khóa thực tế lớn nhất là bao nhiêu
            actual_hold_times = []
            for node in nodes.values():
                if tx_id in node.lock_hold_durations:
                    actual_hold_times.append(node.lock_hold_durations[tx_id])
            
            # Nếu không có site nào lấy khóa (do Vote Abort ngay từ đầu do lỗi DB), thời gian block là 0
            if actual_hold_times:
                blocking_times.append(max(actual_hold_times))
            else:
                blocking_times.append(0.0)
                
        success_rate = (commits / num_tests) * 100
        avg_blocking = sum(blocking_times) / len(blocking_times)
        
        results.append({
            'Lease_Duration_ms': lease, 
            'Success_Rate_%': success_rate,
            'Avg_Blocking_Time_ms': avg_blocking
        })
        print(f"Lease: {lease}ms | Success Rate: {success_rate:.2f}% | Avg Resource Block: {avg_blocking:.1f}ms")

    df = pd.DataFrame(results)
    
    # --- VẼ BIỂU ĐỒ KÉP THỂ HIỆN SỰ ĐÁNH ĐỔI (TRADE-OFF) CHUẨN ACADEMIC ---
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Subplot 1: Success Rate vs Lease Duration
    ax1.plot(df['Lease_Duration_ms'], df['Success_Rate_%'], marker='o', linestyle='-', color='#2ca02c', linewidth=2.5, label='Success Rate')
    ax1.axvline(x=120, color='r', linestyle='--', label='Sweet Spot (~120ms)')
    ax1.set_title('Success Rate (%) vs. Lease Duration (ms)', fontsize=11, fontweight='bold')
    ax1.set_xlabel('Lease Duration (ms)')
    ax1.set_ylabel('Transaction Success Rate (%)')
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend()
    
    # Subplot 2: Blocking Latency vs Lease Duration
    ax2.plot(df['Lease_Duration_ms'], df['Avg_Blocking_Time_ms'], marker='s', linestyle='-', color='#ff7f0e', linewidth=2.5, label='Blocking Latency')
    ax2.axvline(x=120, color='r', linestyle='--', label='Sweet Spot (~120ms)')
    ax2.set_title('Avg Resource Locking Time (ms) vs. Lease Duration (ms)', fontsize=11, fontweight='bold')
    ax2.set_xlabel('Lease Duration (ms)')
    ax2.set_ylabel('Resource Lock Time (ms)')
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.legend()
    
    plt.suptitle('THE TWO-PHASE COMMIT LEASE DURATION TRADE-OFF ANALYSIS', fontsize=14, fontweight='bold', y=0.98)
    chart_path = 'lease_analysis.png'
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    print(f"\nExperiment complete. Dual-trade-off Chart saved to {chart_path}")
    
    # Pha Bơm Dữ Liệu Toàn Tải (Full Ingestion Phase) dùng Lease tối ưu 120ms
    print("\n--- PHASE 5: FULL INGESTION OF 10,000 SENSOR READINGS ---")
    print("Ingesting all 10,000 source readings using the optimal Lease duration of 120ms...")
    
    # Dọn dẹp dữ liệu đĩa của các Site trước khi bắt đầu bơm toàn tải
    for name in ['Site_North', 'Site_Central', 'Site_South']:
        f = f"{name}_storage.csv"
        if os.path.exists(f):
            os.remove(f)
            
    ingest_nodes = {
        'Site_North': ParticipantNode('Site_North', db_fail_rate=0.03),
        'Site_Central': ParticipantNode('Site_Central', db_fail_rate=0.03),
        'Site_South': ParticipantNode('Site_South', db_fail_rate=0.03)
    }
    ingest_coord = Coordinator(ingest_nodes)
    
    batch_size = 100
    total_records = len(df_dataset)
    num_batches = total_records // batch_size
    
    success_count = 0
    abort_count = 0
    
    for b in range(num_batches):
        batch_tx_id = f"TX-INGEST-{b}"
        batch_records = df_dataset.iloc[b * batch_size : (b + 1) * batch_size].to_dict('records')
        
        # Bơm qua Coordinator với Lease tối ưu 120ms, trễ mạng nhẹ (5ms - 15ms)
        status = ingest_coord.execute_transaction(
            batch_tx_id, batch_records, lease_duration=120, network_delay_range=(5, 15)
        )
        if status == "COMMITTED":
            success_count += 1
        else:
            abort_count += 1
            
    print(f"Full Ingestion Completed! Batches Committed: {success_count}/{num_batches} | Batches Aborted: {abort_count}/{num_batches}")
    print("Physical records successfully horizontally fragmented and stored:")
    for name, node in ingest_nodes.items():
        storage_file = f"{name}_storage.csv"
        if os.path.exists(storage_file):
            lines = len(open(storage_file).readlines()) - 1
            print(f"- Node {name} physically stored {lines} records in '{storage_file}'")
            
    # Demonstrate failure case (Coordinator Crash / Network Partition)
    print("\n--- SIMULATING FAILURE SCENARIO: Coordinator Crash after PREPARE ---")
    nodes_f = {
        'Site_North': ParticipantNode('Site_North'),
        'Site_Central': ParticipantNode('Site_Central'),
        'Site_South': ParticipantNode('Site_South')
    }
    coord_f = Coordinator(nodes_f)
    
    batch_f = df_dataset.head(3).to_dict('records')
    
    status_f = coord_f.execute_transaction("TX-FAIL-1", batch_f, lease_duration=100, network_delay_range=(10, 20), failure_scenario='coordinator_crash_after_prepare')
    print(f"Transaction Status: {status_f}")
    
    print("\n[SYSTEM NOTICE] Sleeping to simulate passage of time (150ms) to trigger Lease Expirations...")
    time.sleep(0.15)
    
    print("\nChecking Node states and active locks after timeout:")
    for name, node in nodes_f.items():
        node.check_lease_expiry("TX-FAIL-1")
        print(f"Node {node.name} active locks: {node.lock_table} | State: {node.transaction_states.get('TX-FAIL-1')}")
        
    print("\nChecking local storage files of nodes (committed data):")
    for name in ['Site_North', 'Site_Central', 'Site_South']:
        storage_file = f"{name}_storage.csv"
        if os.path.exists(storage_file):
            lines = len(open(storage_file).readlines()) - 1
            print(f"- Node {name} physically stored {lines} records in local file '{storage_file}'")
        else:
            print(f"- Node {name} has no stored data (all aborted or no commits)")
            
    return df

if __name__ == "__main__":
    run_experiment()
