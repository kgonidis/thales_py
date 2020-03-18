from .minio import get_base, check_base
import os
from shutil import copyfile


lidar_folder = '/media/data/minio/ngps-processing/lidar'
from_folder = '/media/data/ngps_processing'
to_folder = '/media/data/minio/ngps-processing'
data_folders = {
    'ats': {
        'folder': 'atsgt',
        'ext': ['_ats.csv']
    },
    'run': {
        'folder': 'runs',
        'ext': ['.p']
    },
    'sparse': {
        'folder': 'ngps-sparse_uwb_1.21',
        'ext': ['.csv', '.imu.csv', '.uwb.csv']
    },
    'dense': {
        'folder': 'ngps-dense_uwb_1.20',
        'ext': ['.csv', '.imu.csv', '.uwb.csv']
    }
}


def get_lidar_files():
    for root, dirs, files in os.walk(lidar_folder):
        for f in files:
            base = get_base(f)
            if base:
                yield { 'file': f, 'full': os.path.join(lidar_folder, f), 'base': base }


def import_runs(data_folders=data_folders):
    for mfile in get_lidar_files():
        base = mfile['base']
        for key in data_folders:
            sub = data_folders[key]['folder']
            for ext in data_folders[key]['ext']:
                from_file = os.path.join(from_folder, sub, base + ext)
                if os.path.isfile(from_file):
                    to_file = os.path.join(to_folder, sub, base + ext)
                    copyfile(from_file, to_file)

