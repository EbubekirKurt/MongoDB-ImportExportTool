# MongoDB Desktop Export/Import Tool

A professional Python desktop app to manage MongoDB databases on `localhost:27017`:

- Connect and list available databases
- Multi-select databases
- Export selected databases to JSON files (grouped by database)
- Import databases from an exported folder back into MongoDB

## Requirements

- Python 3.10+
- MongoDB running locally on port `27017`

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python app.py
```

## How Export Works

- Choose one or more databases in the UI
- Click **Export Selected Databases**
- Choose a destination folder
- The app creates:
  - `<destination>/<database>_db/<collection>.json`

## How Import Works

- Click **Import Databases From Folder**
- Select a folder that contains exported database directories
- The app replaces each matching collection with the JSON data from file
