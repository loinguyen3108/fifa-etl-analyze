<img src="https://github.com/loinguyen3108/fifa-etl-analyze/blob/main/images/fifa-logo.jpg?raw=true" alt="Fifa Player" width="500"/>

> This is project ETL data from csv files to hive. Then, this data will be analyze with superset

[![github release date](https://img.shields.io/github/release-date/loinguyen3108/fifa-etl-analyze)](https://github.com/loinguyen3108/fifa-etl-analyze/releases/tag/latest) [![commit active](https://img.shields.io/github/commit-activity/w/loinguyen3108/fifa-etl-analyze)](https://github.com/loinguyen3108/fifa-etl-analyze/commit/main) [![license](https://img.shields.io/badge/license-Apache-blue)](https://github.com/nhn/tui.editor/blob/master/LICENSE) [![PRs welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/loinguyen3108/fifa-etl-analyze/issues) [![code with hearth by Loi Nguyen](https://img.shields.io/badge/DE-Loi%20Nguyen-orange)](https://github.com/loinguyen3108)

## 🚩 Table of Contents
- [🚩 Table of Contents](#-table-of-contents)
- [🎨 Stack](#-stack)
  - [⚙️ Setup](#️-setup)
- [⭐ Fifa Star Schema](#fifa-star-schema)
- [✍️ Example](#️-example)
- [📜 License](#-license)

## 🎨 Stack

Project run in local based on `docker-compose.yml` in [bigdata-stack](https://github.com/loinguyen3108/bigdata-stack)

### ⚙️ Setup

**1. Run bigdata-stack**
```
git clone git@github.com:loinguyen3108/bigdata-stack.git

cd bigdata-stack

docker compose up -d

# setup superset
# 1. Setup your local admin account

docker exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

2. Migrate local DB to latest

docker exec -it superset superset db upgrade

3. Load Examples

docker exec -it superset superset load_examples

4. Setup roles

docker exec -it superset superset init

Login and take a look -- navigate to http://localhost:8080/login/ -- u/p: [admin/admin]
```

**2. Spark Standalone**  
Setup at [spark document](https://spark.apache.org/docs/latest/spark-standalone.html)

**3. Dataset**  
Data is downloaded at [Fifa Dataset](https://drive.google.com/file/d/1BKEHD8FaTD3uLKU0dU9w-4SVs_4z1RmA/view?usp=sharing)

**4. Environment**
```
export JDBC_URL=...
export JDBC_USER=...
export JDBC_PASSWORD=...
```

**5. Build dependencies**
```
./build_dependencies.sh
```

**6. Insert local packages**
```
./update_local_packages.sh
```

**7. Args help**
```
cd manager
python ingestion.py -h
python transform.py -h
cd ..
```

**8. Run**
```
# ingest data from postgres to datalake
spark-submit --py-files packages.zip manager/ingestion.py --file file:/home/... --fifa-version <version> --gender <0 or 1> --table_name <table_name>

# transform data from datalake to hive
# Init dim_date
spark-submit --py-files packages.zip manager/transform .py--init --exec-date YYYY:MM:DD

#Transform
spark-submit --py-files packages.zip manager/transform.py --fifa-version <version>
```

## ⭐ Fifa Star Schema
[Fifa schema](https://drive.google.com/file/d/1WN8exuq16WHIwoXQAJkXhd-xI7NxO1Go/view?usp=sharing)

## ✍️ Example

- Data Lake
# ![Data Lake](https://github.com/loinguyen3108/fifa-etl-analyze/blob/main/images/datalake.png?raw=true)

- Hive
# ![Hive](https://github.com/loinguyen3108/fifa-etl-analyze/blob/main/images/hvie.png?raw=true)

- Superset
# ![Superset](https://github.com/loinguyen3108/fifa-etl-analyze/blob/main/images/superset.jpg?raw=true)
## 📜 License

This software is licensed under the [Apache](https://github.com/loinguyen3108/dvdrental-etl/blob/master/LICENSE) © [Loi Nguyen](https://github.com/loinguyen3108).
