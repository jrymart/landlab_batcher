from torch.utils.data import Dataset
import sqlite3
import os
import numpy as np

def get_runs(database, filter_query = ""):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.execute(f"SELECT model_run_id FROM model_run_metadata WHERE model_end_time IS NOT NULL {filter_query}")
    return [r[0] for r in cursor.fetchall()]

class LandlabBatchdataset(Dataset):
    def __init__(self, database, dataset_dir, label_query, filter_query=None):
        self.img_db = database
        self.dataset_directory = dataset_dir
        self.connection = sqlite3.connect(database)
        self.cursor = connection.curosr()
        self.label_query = label_query
        if filter_query is not None:
            self.filter_query = filter_query
        else:
            self.filter_query = ""
        self.runs = get_runs(database, filter_query)

    def __len__(self):
        return len(self.img_labels)

    def __get__item(self, idx):
        run_name = self.runs[idx]
        data_path = os.path.join(self.dataset_directory, f"{run_name}.npz")
        label_query = f"{self.label_query} WHERE model_run_id = {run_name}"
        self.cursor.execute(label_query)
        label = self.cursor.fetchone()[0]
        data_array = np.load(data_path)
        return data_array, label
