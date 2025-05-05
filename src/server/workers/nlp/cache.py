import csv
import glob
import hashlib
import os
import threading
import time

class SentimentCache:
    def __init__(self, node_num, path="/app/cache", flush_interval=60):
        self.cache_file = os.path.join(path, f"cache_{node_num}.csv")
        self.db = {}
        self.new_entries = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.flush_interval = flush_interval
        self.thread = threading.Thread(target=self._flusher)
        self.thread.daemon = True

    def __enter__(self):
        for path in glob.glob(os.path.join(os.path.dirname(self.cache_file), "cache_*.csv")):
            with open(path, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    self.db[row[0]] = row[1]
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        self.thread.join()
        self.flush()

    def _flusher(self):
        while not self._stop.is_set():
            time.sleep(self.flush_interval)
            self.flush()

    def flush(self):
        with self.lock:
            if not self.new_entries:
                return
            with open(self.cache_file, "a+", newline='') as f:
                writer = csv.writer(f)
                for k, v in self.new_entries.items():
                    writer.writerow([k, v])
            self.new_entries.clear()

    def get(self, movie):
        key = self.get_hash(movie)
        return self.db.get(key, None)

    def set(self, movie, label):
        key = self.get_hash(movie)
        with self.lock:
            if key not in self.db:
                self.db[key] = label
                self.new_entries[key] = label

    @staticmethod
    def get_hash(movie):
        return str(hashlib.sha256(movie.overview[:64].encode()).hexdigest())
