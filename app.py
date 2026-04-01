import json
import threading
from collections.abc import Mapping
from pathlib import Path
from tkinter import END, MULTIPLE, Listbox, StringVar, Text, Tk, filedialog, messagebox
from tkinter import ttk

from bson import json_util
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDesktopTool:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("MongoDB Database Export/Import Tool")
        self.root.geometry("900x600")
        self.root.minsize(820, 520)

        self.connection_string_var = StringVar(value="mongodb://localhost:27017")
        self.status_var = StringVar(value="Ready.")
        self.import_mode_var = StringVar(value="folder_name")
        self.import_strategy_var = StringVar(value="replace")
        self.target_db_var = StringVar(value="")
        self.client: MongoClient | None = None
        self.database_names: list[str] = []

        self._configure_theme()
        self._build_ui()

    def _configure_theme(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        self.colors = {
            "bg": "#f4f7fb",
            "card": "#ffffff",
            "primary": "#2f6fed",
            "primary_hover": "#2a62d0",
            "muted_text": "#5f6b7a",
            "heading": "#1f2937",
            "border": "#d4dbe6",
            "success": "#0f9d58",
        }

        self.root.configure(bg=self.colors["bg"])
        style.configure("Root.TFrame", background=self.colors["bg"])
        style.configure("Card.TFrame", background=self.colors["card"], relief="flat")
        style.configure(
            "Title.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["heading"],
            font=("Segoe UI", 19, "bold"),
        )
        style.configure(
            "Muted.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["muted_text"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "CardHeading.TLabel",
            background=self.colors["card"],
            foreground=self.colors["heading"],
            font=("Segoe UI", 10, "bold"),
        )
        style.configure(
            "CardBody.TLabel",
            background=self.colors["card"],
            foreground=self.colors["muted_text"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "Status.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["success"],
            font=("Segoe UI", 9, "bold"),
        )
        style.configure(
            "Primary.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground="white",
            background=self.colors["primary"],
            borderwidth=0,
            focusthickness=3,
            focuscolor=self.colors["primary"],
            padding=(14, 10),
        )
        style.map(
            "Primary.TButton",
            background=[("active", self.colors["primary_hover"]), ("disabled", "#b8c6e8")],
            foreground=[("disabled", "#f3f6ff")],
        )
        style.configure(
            "Secondary.TButton",
            font=("Segoe UI", 10),
            foreground=self.colors["heading"],
            background="#edf2fb",
            borderwidth=0,
            padding=(12, 9),
        )
        style.map(
            "Secondary.TButton",
            background=[("active", "#e1e9f8"), ("disabled", "#f1f5fb")],
            foreground=[("disabled", "#8a95a8")],
        )
        style.configure("TScrollbar", background=self.colors["card"])

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, style="Root.TFrame", padding=18)
        container.pack(fill="both", expand=True)

        title = ttk.Label(container, text="MongoDB Localhost Tool", style="Title.TLabel")
        title.pack(anchor="w")

        subtitle = ttk.Label(
            container,
            text="Connect, select databases, export to JSON, and import back safely.",
            style="Muted.TLabel",
        )
        subtitle.pack(anchor="w", pady=(2, 14))

        conn_card = ttk.Frame(container, style="Card.TFrame", padding=14)
        conn_card.pack(fill="x", pady=(0, 12))

        conn_frame = ttk.Frame(conn_card, style="Card.TFrame")
        conn_frame.pack(fill="x")

        ttk.Label(conn_frame, text="Connection URI", style="CardHeading.TLabel").pack(side="left")
        self.connection_entry = ttk.Entry(
            conn_frame, textvariable=self.connection_string_var, width=64, font=("Consolas", 10)
        )
        self.connection_entry.pack(side="left", padx=8, fill="x", expand=True)

        self.connect_button = ttk.Button(
            conn_frame,
            text="Connect & Load Databases",
            command=self.connect_and_load,
            style="Primary.TButton",
        )
        self.connect_button.pack(side="left")

        middle = ttk.Frame(container, style="Root.TFrame")
        middle.pack(fill="both", expand=True)

        left_panel = ttk.Frame(middle, style="Card.TFrame", padding=14)
        left_panel.pack(side="left", fill="both", expand=True, padx=(0, 8))

        ttk.Label(left_panel, text="Databases", style="CardHeading.TLabel").pack(anchor="w")
        ttk.Label(
            left_panel, text="Multi-select supported (Ctrl/Shift).", style="CardBody.TLabel"
        ).pack(anchor="w", pady=(0, 8))

        list_frame = ttk.Frame(left_panel, style="Card.TFrame")
        list_frame.pack(fill="both", expand=True, pady=(4, 8))

        self.db_listbox = Listbox(
            list_frame,
            selectmode=MULTIPLE,
            exportselection=False,
            font=("Consolas", 10),
            borderwidth=1,
            relief="solid",
            background="#fcfdff",
            foreground=self.colors["heading"],
            selectbackground="#cfe0ff",
            selectforeground=self.colors["heading"],
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            activestyle="none",
        )
        self.db_listbox.pack(side="left", fill="both", expand=True)

        list_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.db_listbox.yview)
        list_scroll.pack(side="right", fill="y")
        self.db_listbox.config(yscrollcommand=list_scroll.set)

        selection_actions = ttk.Frame(left_panel, style="Card.TFrame")
        selection_actions.pack(fill="x")
        ttk.Button(
            selection_actions, text="Select All", command=self.select_all, style="Secondary.TButton"
        ).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(
            selection_actions,
            text="Clear Selection",
            command=self.clear_selection,
            style="Secondary.TButton",
        ).pack(side="left")

        right_panel = ttk.Frame(middle, style="Card.TFrame", padding=14)
        right_panel.pack(side="left", fill="y")

        ttk.Label(right_panel, text="Actions", style="CardHeading.TLabel").pack(anchor="w", pady=(0, 8))

        self.export_button = ttk.Button(
            right_panel,
            text="Export Selected Databases",
            command=self.export_selected_databases,
            state="disabled",
            width=32,
            style="Primary.TButton",
        )
        self.export_button.pack(fill="x", pady=(0, 8))

        self.import_button = ttk.Button(
            right_panel,
            text="Import Databases From Folder",
            command=self.import_from_folder,
            state="disabled",
            width=32,
            style="Secondary.TButton",
        )
        self.import_button.pack(fill="x", pady=(0, 10))

        import_mode_frame = ttk.Frame(right_panel, style="Card.TFrame")
        import_mode_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(import_mode_frame, text="Import Mode", style="CardHeading.TLabel").pack(anchor="w")
        ttk.Radiobutton(
            import_mode_frame,
            text="Use folder database names",
            variable=self.import_mode_var,
            value="folder_name",
            command=self._on_import_mode_change,
        ).pack(anchor="w")
        ttk.Radiobutton(
            import_mode_frame,
            text="Import into existing database",
            variable=self.import_mode_var,
            value="existing",
            command=self._on_import_mode_change,
        ).pack(anchor="w")

        target_db_frame = ttk.Frame(right_panel, style="Card.TFrame")
        target_db_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(target_db_frame, text="Target DB", style="CardBody.TLabel").pack(anchor="w")
        self.target_db_combo = ttk.Combobox(
            target_db_frame,
            textvariable=self.target_db_var,
            state="disabled",
            values=[],
        )
        self.target_db_combo.pack(fill="x", pady=(4, 0))

        import_strategy_frame = ttk.Frame(right_panel, style="Card.TFrame")
        import_strategy_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(
            import_strategy_frame, text="Import Strategy", style="CardHeading.TLabel"
        ).pack(anchor="w")
        ttk.Radiobutton(
            import_strategy_frame,
            text="Replace collections (clear then insert)",
            variable=self.import_strategy_var,
            value="replace",
        ).pack(anchor="w")
        ttk.Radiobutton(
            import_strategy_frame,
            text="Merge/Upsert by _id",
            variable=self.import_strategy_var,
            value="merge",
        ).pack(anchor="w")

        self.progress = ttk.Progressbar(right_panel, mode="indeterminate")
        self.progress.pack(fill="x", pady=(0, 10))

        ttk.Label(right_panel, text="Status Log", style="CardHeading.TLabel").pack(anchor="w")
        self.log_text = Text(
            right_panel,
            height=18,
            width=42,
            wrap="word",
            borderwidth=1,
            relief="solid",
            background="#fbfcff",
            foreground="#1f2a3a",
            insertbackground="#1f2a3a",
            padx=8,
            pady=8,
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.config(state="disabled")

        footer = ttk.Frame(container, style="Root.TFrame")
        footer.pack(fill="x", pady=(8, 0))
        ttk.Label(footer, textvariable=self.status_var, style="Status.TLabel").pack(side="left")

    def log(self, message: str) -> None:
        self.log_text.config(state="normal")
        self.log_text.insert(END, f"{message}\n")
        self.log_text.see(END)
        self.log_text.config(state="disabled")
        self.status_var.set(message)

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.connect_button.config(state=state)
        self.export_button.config(state=state if self.client else "disabled")
        self.import_button.config(state=state if self.client else "disabled")
        if busy:
            self.progress.start(8)
        else:
            self.progress.stop()

    def connect_and_load(self) -> None:
        uri = self.connection_string_var.get().strip()
        if not uri:
            messagebox.showerror("Missing URI", "Please enter a MongoDB URI.")
            return

        def worker() -> None:
            self.root.after(0, lambda: self._set_busy(True))
            try:
                client = MongoClient(uri, serverSelectionTimeoutMS=4000)
                client.admin.command("ping")
                names = sorted(client.list_database_names())

                def on_success() -> None:
                    self.client = client
                    self.database_names = names
                    self.db_listbox.delete(0, END)
                    for db_name in names:
                        self.db_listbox.insert(END, db_name)
                    self.export_button.config(state="normal")
                    self.import_button.config(state="normal")
                    self.target_db_combo.config(values=names)
                    if names:
                        self.target_db_var.set(names[0])
                    self._on_import_mode_change()
                    self.log(f"Connected. Loaded {len(names)} databases.")
                    self._set_busy(False)

                self.root.after(0, on_success)
            except PyMongoError as exc:
                self.root.after(
                    0,
                    lambda: (
                        self.log(f"Connection failed: {exc}"),
                        self._set_busy(False),
                        messagebox.showerror("Connection Error", str(exc)),
                    ),
                )

        threading.Thread(target=worker, daemon=True).start()

    def get_selected_databases(self) -> list[str]:
        selected_indices = self.db_listbox.curselection()
        return [self.db_listbox.get(i) for i in selected_indices]

    def select_all(self) -> None:
        if not self.database_names:
            return
        self.db_listbox.select_set(0, END)

    def clear_selection(self) -> None:
        self.db_listbox.selection_clear(0, END)

    def export_selected_databases(self) -> None:
        if not self.client:
            return

        selected_dbs = self.get_selected_databases()
        if not selected_dbs:
            messagebox.showwarning("No Selection", "Select at least one database to export.")
            return

        destination = filedialog.askdirectory(title="Choose export destination")
        if not destination:
            return

        export_root = Path(destination)

        def worker() -> None:
            self.root.after(0, lambda: self._set_busy(True))
            try:
                for db_name in selected_dbs:
                    db_folder = export_root / f"{db_name}_db"
                    db_folder.mkdir(parents=True, exist_ok=True)
                    db = self.client[db_name]
                    for collection_name in db.list_collection_names():
                        docs = list(db[collection_name].find({}))
                        file_path = db_folder / f"{collection_name}.json"
                        with open(file_path, "w", encoding="utf-8") as f:
                            json.dump(docs, f, default=json_util.default, indent=2)

                self.root.after(
                    0,
                    lambda: (
                        self.log(
                            f"Export completed: {len(selected_dbs)} database(s) to {export_root}"
                        ),
                        self._set_busy(False),
                        messagebox.showinfo("Export Complete", "Database export finished successfully."),
                    ),
                )
            except (PyMongoError, OSError, TypeError, ValueError) as exc:
                self.root.after(
                    0,
                    lambda: (
                        self.log(f"Export failed: {exc}"),
                        self._set_busy(False),
                        messagebox.showerror("Export Error", str(exc)),
                    ),
                )

        threading.Thread(target=worker, daemon=True).start()

    def import_from_folder(self) -> None:
        if not self.client:
            return

        if self.import_mode_var.get() == "existing":
            source_folder = filedialog.askdirectory(
                title="Choose exported database folder (contains collection JSON files)"
            )
        else:
            source_folder = filedialog.askdirectory(
                title="Choose folder containing exported database directories"
            )
        if not source_folder:
            return

        confirm = messagebox.askyesno(
            "Confirm Import",
            f"Import strategy: {self.import_strategy_var.get()}.\n\nContinue?",
        )
        if not confirm:
            return

        import_root = Path(source_folder)

        def worker() -> None:
            self.root.after(0, lambda: self._set_busy(True))
            try:
                imported_db_count = 0
                summary = {"inserted": 0, "upserted": 0, "replaced": 0, "cleared": 0}
                if self.import_mode_var.get() == "existing":
                    target_db_name = self.target_db_var.get().strip()
                    if not target_db_name:
                        raise ValueError("Please select a target database.")

                    json_files = sorted(import_root.glob("*.json"))
                    if not json_files:
                        db_dirs_with_json = [
                            d for d in import_root.iterdir() if d.is_dir() and list(d.glob("*.json"))
                        ]
                        if len(db_dirs_with_json) == 1:
                            json_files = sorted(db_dirs_with_json[0].glob("*.json"))
                        elif len(db_dirs_with_json) > 1:
                            raise ValueError(
                                "Selected folder contains multiple database folders. "
                                "Pick a single database folder."
                            )

                    if not json_files:
                        raise ValueError("No collection JSON files found in selected folder.")

                    db = self.client[target_db_name]
                    for json_file in json_files:
                        collection_name = json_file.stem
                        with open(json_file, "r", encoding="utf-8") as f:
                            docs = json.load(f, object_hook=json_util.object_hook)

                        if not isinstance(docs, list):
                            raise ValueError(f"{json_file} must contain a JSON array of documents.")

                        collection = db[collection_name]
                        collection_summary = self._write_collection(collection, docs)
                        for key, value in collection_summary.items():
                            summary[key] += value
                        self.root.after(
                            0,
                            lambda c=collection_name, s=collection_summary: self.log(
                                f"[{target_db_name}.{c}] inserted={s['inserted']} "
                                f"upserted={s['upserted']} replaced={s['replaced']} cleared={s['cleared']}"
                            ),
                        )
                    imported_db_count = 1
                else:
                    database_dirs = [d for d in import_root.iterdir() if d.is_dir()]
                    if not database_dirs:
                        raise ValueError("No database directories were found in selected folder.")

                    for db_dir in database_dirs:
                        db_name = db_dir.name
                        if db_name.endswith("_db"):
                            db_name = db_name[:-3]
                        db = self.client[db_name]
                        json_files = sorted(db_dir.glob("*.json"))
                        if not json_files:
                            continue

                        for json_file in json_files:
                            collection_name = json_file.stem
                            with open(json_file, "r", encoding="utf-8") as f:
                                docs = json.load(f, object_hook=json_util.object_hook)

                            if not isinstance(docs, list):
                                raise ValueError(f"{json_file} must contain a JSON array of documents.")

                            collection = db[collection_name]
                            collection_summary = self._write_collection(collection, docs)
                            for key, value in collection_summary.items():
                                summary[key] += value
                            self.root.after(
                                0,
                                lambda d=db_name, c=collection_name, s=collection_summary: self.log(
                                    f"[{d}.{c}] inserted={s['inserted']} "
                                    f"upserted={s['upserted']} replaced={s['replaced']} cleared={s['cleared']}"
                                ),
                            )
                        imported_db_count += 1

                self.root.after(
                    0,
                    lambda: (
                        self.log(
                            f"Import completed: {imported_db_count} database(s). "
                            f"Totals -> inserted={summary['inserted']}, "
                            f"upserted={summary['upserted']}, replaced={summary['replaced']}, "
                            f"cleared={summary['cleared']}"
                        ),
                        self._set_busy(False),
                        messagebox.showinfo("Import Complete", "Database import finished successfully."),
                    ),
                )
            except (PyMongoError, OSError, json.JSONDecodeError, ValueError) as exc:
                self.root.after(
                    0,
                    lambda: (
                        self.log(f"Import failed: {exc}"),
                        self._set_busy(False),
                        messagebox.showerror("Import Error", str(exc)),
                    ),
                )

        threading.Thread(target=worker, daemon=True).start()

    def _on_import_mode_change(self) -> None:
        is_existing_mode = self.import_mode_var.get() == "existing"
        if is_existing_mode and self.client:
            self.target_db_combo.config(state="readonly")
            if not self.target_db_var.get() and self.database_names:
                self.target_db_var.set(self.database_names[0])
        else:
            self.target_db_combo.config(state="disabled")

    def _write_collection(self, collection, docs: list[dict]) -> dict[str, int]:
        result = {"inserted": 0, "upserted": 0, "replaced": 0, "cleared": 0}
        if self.import_strategy_var.get() == "replace":
            deleted_result = collection.delete_many({})
            result["cleared"] = deleted_result.deleted_count
            if docs:
                collection.insert_many(docs)
                result["replaced"] = len(docs)
            return result

        # Merge mode: upsert documents with _id, insert documents without _id.
        for doc in docs:
            if isinstance(doc, Mapping) and "_id" in doc:
                collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                result["upserted"] += 1
            else:
                collection.insert_one(doc)
                result["inserted"] += 1
        return result


def main() -> None:
    root = Tk()
    app = MongoDesktopTool(root)
    app.log("Ready. Connect to MongoDB to begin.")
    root.mainloop()


if __name__ == "__main__":
    main()
