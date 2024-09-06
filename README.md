# 1BRC: One Billion Row Challenge in Python


## How to create your input files

### 1) Create your Python virtual environment and activate it

```shell
strictly-come-coding % python3 -m venv venv
strictly-come-coding % . venv/bin/activate
(venv) strictly-come-coding % which pip
/Users/bryan.williams/development/strictly-come-coding/venv/bin/pip
```


### 2) Install the requirements for the creator / reference results generator

```shell
(venv) strictly-come-coding % pip install -r requirements.txt
Collecting numpy>=1.24.2 (from -r requirements.txt (line 1))
  Downloading numpy-2.1.1-cp312-cp312-macosx_14_0_arm64.whl.metadata (60 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 60.9/60.9 kB 4.0 MB/s eta 0:00:00
Collecting polars>=0.20.0 (from -r requirements.txt (line 2))
  Downloading polars-1.6.0-cp38-abi3-macosx_11_0_arm64.whl.metadata (14 kB)
Collecting tqdm>=4.66.0 (from -r requirements.txt (line 3))
  Using cached tqdm-4.66.5-py3-none-any.whl.metadata (57 kB)
Downloading numpy-2.1.1-cp312-cp312-macosx_14_0_arm64.whl (5.1 MB)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 5.1/5.1 MB 67.4 MB/s eta 0:00:00
Downloading polars-1.6.0-cp38-abi3-macosx_11_0_arm64.whl (26.8 MB)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 26.8/26.8 MB 94.5 MB/s eta 0:00:00
Using cached tqdm-4.66.5-py3-none-any.whl (78 kB)
Installing collected packages: tqdm, polars, numpy
Successfully installed numpy-2.1.1 polars-1.6.0 tqdm-4.66.5

[notice] A new release of pip is available: 24.0 -> 24.2
[notice] To update, run: pip install --upgrade pip
```

### 3) Generate an example input for with a seed of 1234

```shell
(venv) strictly-come-coding % python createMeasurements.py --seed 1234
/Users/bryan.williams/development/strictly-come-coding/createMeasurements.py:426: DataOrientationWarning: Row orientation inferred during DataFrame construction. Explicitly specify the orientation by passing `orient="row"` to silence this warning.
  stations = pl.DataFrame(STATIONS, ("names", "means"))
Creating measurement file 'measurements.txt' with 1,000,000,000 measurements...
100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 100/100 [00:33<00:00,  2.97it/s]
Created file 'measurements.txt' with 1,000,000,000 measurements in 33.75 seconds
````




