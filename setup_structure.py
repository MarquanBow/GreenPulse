import os

folders = [
    "data/raw",
    "data/processed",
    "notebooks",
    "src",
    "app",
    "plots"
]

files = {
    "requirements.txt": "",
    "README.md": "# GreenPulse\n\nUrban Air & Water Quality Analytics Dashboard.",
    ".env": "",
    "src/data_fetcher.py": "",
    "src/data_cleaner.py": "",
    "src/visualizer.py": "",
    "src/utils.py": "",
    "app/app.py": "",
    "notebooks/exploratory_analysis.ipynb": ""
}

for folder in folders:
    os.makedirs(folder, exist_ok=True)

for file_path, content in files.items():
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

print("✅ Project structure created successfully!")