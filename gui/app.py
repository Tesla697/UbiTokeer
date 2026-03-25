import json
import logging
import queue
import threading
import tkinter as tk
from pathlib import Path
from typing import Callable, Optional

import customtkinter as ctk

CONFIG_PATH = Path(__file__).parent.parent / "config.json"
ACCOUNTS_PATH = Path(__file__).parent.parent / "accounts.json"
GAME_NAMES_PATH = Path(__file__).parent.parent / "game_names.json"

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

STATUS_COLORS = {
    "idle": "#4caf50",
    "busy": "#ffa500",
    "error": "#f44336",
    "stopped": "#888888",
}


class GuiLogHandler(logging.Handler):
    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self._q = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._q.put_nowait(record)
        except queue.Full:
            pass


class UbiTokeerApp(ctk.CTk):
    def __init__(
        self,
        config: dict,
        on_save_config: Callable[[dict], None],
        on_toggle_server: Callable[[bool], None],
    ):
        super().__init__()

        self._config = config
        self._on_save_config = on_save_config
        self._on_toggle_server = on_toggle_server
        self._server_running = False
        self._log_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._queue_state: dict = {"current": None, "pending": None}
        self._queue_state_lock = threading.Lock()
        self._quota_tracker = None

        self.title("UbiTokeer Backend")
        self.geometry("820x660")
        self.minsize(720, 540)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_ui()
        self._poll_logs()
        self._poll_state()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # Top bar
        top = ctk.CTkFrame(self, height=46, corner_radius=0)
        top.pack(fill="x", padx=0, pady=0)
        top.pack_propagate(False)

        self._status_dot = ctk.CTkLabel(top, text="\u25cf", font=("Segoe UI", 18), width=24)
        self._status_dot.pack(side="left", padx=(12, 4), pady=8)

        self._status_label = ctk.CTkLabel(top, text="IDLE", font=("Segoe UI", 13, "bold"))
        self._status_label.pack(side="left", padx=(0, 16))

        port = self._config.get("port", 8090)
        self._server_info = ctk.CTkLabel(
            top, text=f"Server: 0.0.0.0:{port}", font=("Segoe UI", 12),
            text_color="#aaaaaa"
        )
        self._server_info.pack(side="left", padx=4)

        self._toggle_btn = ctk.CTkButton(
            top, text="Start Server", width=110, height=30,
            command=self._toggle_server
        )
        self._toggle_btn.pack(side="right", padx=12, pady=8)

        # Tab view
        self._tabs = ctk.CTkTabview(self)
        self._tabs.pack(fill="both", expand=True, padx=10, pady=(6, 10))
        self._tabs.add("Dashboard")
        self._tabs.add("Quota")
        self._tabs.add("Config")

        self._build_dashboard(self._tabs.tab("Dashboard"))
        self._build_quota_tab(self._tabs.tab("Quota"))
        self._build_config_tab(self._tabs.tab("Config"))

        self._set_status("idle")

    def _build_dashboard(self, parent: ctk.CTkFrame) -> None:
        # Job info row
        info_row = ctk.CTkFrame(parent)
        info_row.pack(fill="x", padx=4, pady=(6, 4))

        # Current job panel
        job_frame = ctk.CTkFrame(info_row)
        job_frame.pack(side="left", fill="both", expand=True, padx=(0, 4))

        ctk.CTkLabel(job_frame, text="Current Job", font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=10, pady=(8, 2)
        )
        self._lbl_uplay_id = self._info_row(job_frame, "Uplay ID", "\u2014")
        self._lbl_account = self._info_row(job_frame, "Account", "\u2014")
        self._lbl_job_status = self._info_row(job_frame, "Status", "\u2014")

        # Queue panel
        queue_frame = ctk.CTkFrame(info_row, width=180)
        queue_frame.pack(side="right", fill="y", padx=(4, 0))
        queue_frame.pack_propagate(False)

        ctk.CTkLabel(queue_frame, text="Queue", font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=10, pady=(8, 2)
        )
        self._lbl_pending = self._info_row(queue_frame, "Pending", "empty")

        # Log panel
        log_frame = ctk.CTkFrame(parent)
        log_frame.pack(fill="both", expand=True, padx=4, pady=(4, 4))

        log_header = ctk.CTkFrame(log_frame, fg_color="transparent")
        log_header.pack(fill="x", padx=6, pady=(6, 2))
        ctk.CTkLabel(log_header, text="Logs", font=("Segoe UI", 12, "bold")).pack(side="left")
        ctk.CTkButton(
            log_header, text="Clear", width=60, height=24,
            command=self._clear_logs
        ).pack(side="right")

        self._log_box = ctk.CTkTextbox(
            log_frame, font=("Consolas", 11), wrap="word",
            state="disabled", activate_scrollbars=True
        )
        self._log_box.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        self._log_box._textbox.tag_configure("DEBUG", foreground="#888888")
        self._log_box._textbox.tag_configure("INFO", foreground="#ffffff")
        self._log_box._textbox.tag_configure("WARNING", foreground="#f0c040")
        self._log_box._textbox.tag_configure("ERROR", foreground="#ff5555")
        self._log_box._textbox.tag_configure("CRITICAL", foreground="#ff0000")

    # ------------------------------------------------------------------
    # Config tab
    # ------------------------------------------------------------------

    def _build_config_tab(self, parent: ctk.CTkFrame) -> None:
        # Accounts section header
        acc_header = ctk.CTkFrame(parent, fg_color="transparent")
        acc_header.pack(fill="x", padx=10, pady=(10, 4))
        ctk.CTkLabel(
            acc_header, text="Accounts", font=("Segoe UI", 12, "bold")
        ).pack(side="left")

        btn_frame = ctk.CTkFrame(acc_header, fg_color="transparent")
        btn_frame.pack(side="right")
        ctk.CTkButton(
            btn_frame, text="+ Add Account", width=120, height=26,
            command=self._add_account_dialog
        ).pack(side="left", padx=4)
        ctk.CTkButton(
            btn_frame, text="Save Accounts", width=120, height=26,
            command=self._save_accounts
        ).pack(side="left", padx=4)

        # Scrollable accounts table
        self._accounts_frame = ctk.CTkScrollableFrame(parent, height=220)
        self._accounts_frame.pack(fill="both", expand=True, padx=10, pady=(0, 4))
        self._accounts_frame.grid_columnconfigure(0, weight=2)   # Email
        self._accounts_frame.grid_columnconfigure(1, weight=2)   # Folder
        self._accounts_frame.grid_columnconfigure(2, weight=2)   # Uplay IDs
        self._accounts_frame.grid_columnconfigure(3, weight=1)   # Limit
        self._accounts_frame.grid_columnconfigure(4, weight=1)   # Actions

        self._account_rows: list[dict] = []
        self._render_accounts()

        # Separator
        ctk.CTkFrame(parent, height=1, fg_color="#444444").pack(fill="x", padx=10, pady=6)

        # Game Names section
        game_header = ctk.CTkFrame(parent, fg_color="transparent")
        game_header.pack(fill="x", padx=10, pady=(0, 4))
        ctk.CTkLabel(
            game_header, text="Game Names", font=("Segoe UI", 12, "bold")
        ).pack(side="left")
        ctk.CTkButton(
            game_header, text="Save Game Names", width=140, height=26,
            command=self._save_game_names
        ).pack(side="right")

        self._game_names_box = ctk.CTkTextbox(parent, height=80, font=("Consolas", 11))
        self._game_names_box.pack(fill="x", padx=10, pady=(0, 4))
        game_names = self._load_game_names()
        self._game_names_box.insert("0.0", json.dumps(game_names, indent=2))

        # Separator
        ctk.CTkFrame(parent, height=1, fg_color="#444444").pack(fill="x", padx=10, pady=6)

        # Settings section
        ctk.CTkLabel(parent, text="Settings", font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=10, pady=(0, 6)
        )

        settings_frame = ctk.CTkFrame(parent, fg_color="transparent")
        settings_frame.pack(fill="x", padx=10)

        # Port
        port_row = ctk.CTkFrame(settings_frame, fg_color="transparent")
        port_row.pack(fill="x", pady=3)
        ctk.CTkLabel(port_row, text="Port:", width=140, anchor="w").pack(side="left")
        self._entry_port = ctk.CTkEntry(port_row, width=80)
        self._entry_port.insert(0, str(self._config.get("port", 8090)))
        self._entry_port.pack(side="left")

        # Process timeout
        timeout_row = ctk.CTkFrame(settings_frame, fg_color="transparent")
        timeout_row.pack(fill="x", pady=3)
        ctk.CTkLabel(timeout_row, text="Process Timeout (s):", width=140, anchor="w").pack(side="left")
        self._entry_timeout = ctk.CTkEntry(timeout_row, width=80)
        self._entry_timeout.insert(0, str(self._config.get("process_timeout", 60)))
        self._entry_timeout.pack(side="left")

        ctk.CTkButton(
            parent, text="Save Config", width=120, command=self._save_config
        ).pack(anchor="e", padx=10, pady=10)

    def _render_accounts(self) -> None:
        for widget in self._accounts_frame.winfo_children():
            widget.destroy()
        self._account_rows.clear()

        # Header
        for col, text in enumerate(["Email", "Folder", "Uplay IDs", "Limit", ""]):
            ctk.CTkLabel(
                self._accounts_frame, text=text, font=("Segoe UI", 11, "bold")
            ).grid(row=0, column=col, sticky="w", padx=6, pady=2)

        accounts = self._load_accounts()
        for i, acc in enumerate(accounts, start=1):
            # Email (read-only label)
            ctk.CTkLabel(
                self._accounts_frame, text=acc["email"], anchor="w"
            ).grid(row=i, column=0, sticky="w", padx=6, pady=2)

            # Folder
            folder_entry = ctk.CTkEntry(self._accounts_frame, width=180)
            folder_entry.insert(0, acc.get("folder", ""))
            folder_entry.grid(row=i, column=1, sticky="w", padx=6, pady=2)

            # Uplay IDs
            uid_entry = ctk.CTkEntry(self._accounts_frame, width=160)
            uid_entry.insert(0, ", ".join(acc.get("uplay_ids", [])))
            uid_entry.grid(row=i, column=2, sticky="w", padx=6, pady=2)

            # Daily limit
            limit_entry = ctk.CTkEntry(self._accounts_frame, width=50)
            limit_entry.insert(0, str(acc.get("daily_limit", 5)))
            limit_entry.grid(row=i, column=3, sticky="w", padx=6, pady=2)

            # Remove button
            ctk.CTkButton(
                self._accounts_frame, text="\u2715", width=30, height=24,
                fg_color="#f44336", hover_color="#d32f2f",
                command=lambda email=acc["email"]: self._remove_account(email)
            ).grid(row=i, column=4, padx=6, pady=2)

            self._account_rows.append({
                "email": acc["email"],
                "accid": acc.get("accid", ""),
                "folder_entry": folder_entry,
                "uid_entry": uid_entry,
                "limit_entry": limit_entry,
            })

    def _add_account_dialog(self) -> None:
        dialog = ctk.CTkInputDialog(
            text="Enter account email:", title="Add Account"
        )
        email = dialog.get_input()
        if not email or not email.strip():
            return
        email = email.strip()

        accounts = self._load_accounts()
        if any(a["email"] == email for a in accounts):
            logging.getLogger("ubitokeer").warning(f"Account {email} already exists")
            return

        accid_dialog = ctk.CTkInputDialog(
            text="Enter accid (UUID):", title="Account ID"
        )
        accid = (accid_dialog.get_input() or "").strip()

        folder_dialog = ctk.CTkInputDialog(
            text="Enter folder path (e.g. activator_cli/avatar-acc1):", title="Folder"
        )
        folder = (folder_dialog.get_input() or "").strip()

        accounts.append({
            "email": email,
            "accid": accid,
            "folder": folder,
            "uplay_ids": [],
            "daily_limit": 5,
        })
        self._write_accounts(accounts)
        self._render_accounts()
        logging.getLogger("ubitokeer").info(f"Account {email} added (folder={folder})")

    def _remove_account(self, email: str) -> None:
        accounts = self._load_accounts()
        accounts = [a for a in accounts if a["email"] != email]
        self._write_accounts(accounts)
        self._render_accounts()
        logging.getLogger("ubitokeer").info(f"Account {email} removed")

    def _save_accounts(self) -> None:
        accounts = self._load_accounts()
        email_map = {row["email"]: row for row in self._account_rows}

        for acc in accounts:
            row = email_map.get(acc["email"])
            if not row:
                continue
            acc["folder"] = row["folder_entry"].get().strip()
            if row.get("accid"):
                acc["accid"] = row["accid"]
            raw_uids = row["uid_entry"].get().strip()
            acc["uplay_ids"] = [u.strip() for u in raw_uids.split(",") if u.strip()]
            try:
                acc["daily_limit"] = int(row["limit_entry"].get().strip())
            except ValueError:
                acc["daily_limit"] = 5

        self._write_accounts(accounts)
        self._render_accounts()
        logging.getLogger("ubitokeer").info("Accounts saved")

    def _load_accounts(self) -> list[dict]:
        if ACCOUNTS_PATH.exists():
            try:
                data = json.loads(ACCOUNTS_PATH.read_text())
                return data.get("accounts", [])
            except Exception:
                pass
        return []

    def _write_accounts(self, accounts: list[dict]) -> None:
        ACCOUNTS_PATH.write_text(json.dumps({"accounts": accounts}, indent=2))

    def _load_game_names(self) -> dict:
        if GAME_NAMES_PATH.exists():
            try:
                return json.loads(GAME_NAMES_PATH.read_text())
            except Exception:
                pass
        return {}

    def _save_game_names(self) -> None:
        try:
            raw = self._game_names_box.get("0.0", "end").strip()
            data = json.loads(raw)
            GAME_NAMES_PATH.write_text(json.dumps(data, indent=2))
            logging.getLogger("ubitokeer").info("Game names saved")
        except json.JSONDecodeError as e:
            logging.getLogger("ubitokeer").error(f"Invalid JSON in game names: {e}")

    def _save_config(self) -> None:
        try:
            self._config["port"] = int(self._entry_port.get().strip())
        except ValueError:
            pass
        try:
            self._config["process_timeout"] = int(self._entry_timeout.get().strip())
        except ValueError:
            pass

        CONFIG_PATH.write_text(json.dumps(self._config, indent=2))
        self._on_save_config(self._config)
        logging.getLogger("ubitokeer").info("Config saved and reloaded")

        self._server_info.configure(
            text=f"Server: 0.0.0.0:{self._config.get('port', 8090)}"
        )

    # ------------------------------------------------------------------
    # Quota tab
    # ------------------------------------------------------------------

    def _build_quota_tab(self, parent: ctk.CTkFrame) -> None:
        self._quota_scroll = ctk.CTkScrollableFrame(parent)
        self._quota_scroll.pack(fill="both", expand=True, padx=4, pady=4)
        self._quota_placeholder = ctk.CTkLabel(
            self._quota_scroll, text="Loading quota data...",
            text_color="#aaaaaa"
        )
        self._quota_placeholder.pack(pady=20)
        self._quota_structure_key: tuple = ()
        self._quota_labels: dict[tuple[str, str], dict] = {}

    def set_quota_tracker(self, tracker) -> None:
        self._quota_tracker = tracker
        self._poll_quota()

    def _poll_quota(self) -> None:
        if self._quota_tracker:
            self._refresh_quota_panel()
        self.after(1000, self._poll_quota)

    def _get_quota_structure_key(self, uplay_map: dict[str, list[str]]) -> tuple:
        parts = []
        for uid in sorted(uplay_map):
            for email in uplay_map[uid]:
                parts.append((uid, email))
        return tuple(parts)

    def _refresh_quota_panel(self) -> None:
        accounts = self._load_accounts()
        uplay_map: dict[str, list[str]] = {}
        for acc in accounts:
            for uid in acc.get("uplay_ids", []):
                uplay_map.setdefault(uid, []).append(acc["email"])

        if not uplay_map:
            if self._quota_structure_key != ():
                self._quota_structure_key = ()
                self._quota_labels.clear()
                for w in self._quota_scroll.winfo_children():
                    w.destroy()
                ctk.CTkLabel(
                    self._quota_scroll, text="No accounts assigned to any game.",
                    text_color="#aaaaaa"
                ).pack(pady=20)
            return

        new_key = self._get_quota_structure_key(uplay_map)
        game_names = self._load_game_names()

        # Structure changed — full rebuild
        if new_key != self._quota_structure_key:
            self._quota_structure_key = new_key
            self._quota_labels.clear()
            for w in self._quota_scroll.winfo_children():
                w.destroy()
            self._build_quota_widgets(accounts, uplay_map, game_names)
            return

        # Structure same — update labels in place
        import time
        now = time.time()
        for uid, emails in uplay_map.items():
            for email in emails:
                key = (uid, email)
                labels = self._quota_labels.get(key)
                if not labels:
                    continue
                remaining = self._quota_tracker.get_remaining(email, uid)
                acc_data = next((a for a in accounts if a["email"] == email), {})
                daily_limit = acc_data.get("daily_limit", 5)
                used = daily_limit - remaining
                used_color = "#ff5555" if remaining == 0 else "#ffffff"
                labels["used"].configure(text=f"{used}/{daily_limit}", text_color=used_color)

                # Calculate resets_in
                q_key = self._quota_tracker._key(email, uid)
                entry = self._quota_tracker._data.get(q_key)
                if entry and remaining == 0:
                    reset_secs = entry["window_start"] + 86400 - now
                    if reset_secs > 0:
                        from core.quota import _format_duration
                        labels["resets"].configure(text=_format_duration(reset_secs))
                    else:
                        labels["resets"].configure(text="\u2014")
                else:
                    labels["resets"].configure(text="\u2014")

    def _build_quota_widgets(
        self, accounts: list[dict], uplay_map: dict, game_names: dict
    ) -> None:
        import time
        now = time.time()

        for uid, emails in uplay_map.items():
            game_name = game_names.get(uid, f"Uplay {uid}")
            ctk.CTkLabel(
                self._quota_scroll,
                text=f"{game_name} ({uid})",
                font=("Segoe UI", 12, "bold"),
            ).pack(anchor="w", padx=6, pady=(10, 4))

            table = ctk.CTkFrame(self._quota_scroll)
            table.pack(fill="x", padx=6, pady=(0, 4))
            table.grid_columnconfigure(0, weight=3)
            table.grid_columnconfigure(1, weight=1)
            table.grid_columnconfigure(2, weight=1)
            table.grid_columnconfigure(3, weight=1)

            for col, text in enumerate(["Account", "Used", "Resets In", ""]):
                ctk.CTkLabel(
                    table, text=text, font=("Segoe UI", 10, "bold"),
                    text_color="#aaaaaa"
                ).grid(row=0, column=col, sticky="w", padx=6, pady=2)

            for i, email in enumerate(emails, start=1):
                acc = next((a for a in accounts if a["email"] == email), {})
                daily_limit = acc.get("daily_limit", 5)
                remaining = self._quota_tracker.get_remaining(email, uid)
                used = daily_limit - remaining

                ctk.CTkLabel(table, text=email, anchor="w").grid(
                    row=i, column=0, sticky="w", padx=6, pady=1
                )

                used_color = "#ff5555" if remaining == 0 else "#ffffff"
                lbl_used = ctk.CTkLabel(
                    table, text=f"{used}/{daily_limit}", anchor="w",
                    text_color=used_color
                )
                lbl_used.grid(row=i, column=1, sticky="w", padx=6, pady=1)

                # Resets in
                q_key = self._quota_tracker._key(email, uid)
                entry = self._quota_tracker._data.get(q_key)
                resets_text = "\u2014"
                if entry and remaining == 0:
                    reset_secs = entry["window_start"] + 86400 - now
                    if reset_secs > 0:
                        from core.quota import _format_duration
                        resets_text = _format_duration(reset_secs)

                lbl_resets = ctk.CTkLabel(table, text=resets_text, anchor="w")
                lbl_resets.grid(row=i, column=2, sticky="w", padx=6, pady=1)

                self._quota_labels[(uid, email)] = {
                    "used": lbl_used,
                    "resets": lbl_resets,
                }

                btn_frame = ctk.CTkFrame(table, fg_color="transparent")
                btn_frame.grid(row=i, column=3, padx=4, pady=1)

                ctk.CTkButton(
                    btn_frame, text="-", width=28, height=24,
                    command=lambda e=email, u=uid: self._quota_decrement(e, u)
                ).pack(side="left", padx=1)
                ctk.CTkButton(
                    btn_frame, text="+", width=28, height=24,
                    command=lambda e=email, u=uid: self._quota_increment(e, u)
                ).pack(side="left", padx=1)

    def _quota_increment(self, email: str, uplay_id: str) -> None:
        if self._quota_tracker:
            self._quota_tracker.record(email, uplay_id)
            self._refresh_quota_panel()

    def _quota_decrement(self, email: str, uplay_id: str) -> None:
        if self._quota_tracker:
            self._quota_tracker.decrement(email, uplay_id)
            self._refresh_quota_panel()

    # ------------------------------------------------------------------
    # Status / state
    # ------------------------------------------------------------------

    def _set_status(self, status: str, label: str | None = None) -> None:
        color = STATUS_COLORS.get(status, "#888888")
        self._status_dot.configure(text_color=color)
        self._status_label.configure(
            text=(label or status.upper()), text_color=color
        )

    def update_queue_state(self, state: dict) -> None:
        with self._queue_state_lock:
            self._queue_state = state
        self.after(0, self._refresh_job_panel)

    def _refresh_job_panel(self) -> None:
        with self._queue_state_lock:
            state = self._queue_state

        current = state.get("current")
        pending = state.get("pending")

        if current:
            self._lbl_uplay_id.configure(text=current.get("uplay_id", "\u2014"))
            self._lbl_account.configure(text=str(current.get("account_email", "\u2014")))
            self._lbl_job_status.configure(text=current.get("status", "\u2014").upper())
            self._set_status("busy", "PROCESSING")
        else:
            self._lbl_uplay_id.configure(text="\u2014")
            self._lbl_account.configure(text="\u2014")
            self._lbl_job_status.configure(text="\u2014")
            self._set_status("idle" if self._server_running else "stopped")

        self._lbl_pending.configure(
            text=pending.get("uplay_id", "?") if pending else "empty"
        )

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def get_log_handler(self) -> GuiLogHandler:
        return GuiLogHandler(self._log_queue)

    def _poll_logs(self) -> None:
        try:
            while True:
                record = self._log_queue.get_nowait()
                self._append_log(record)
        except queue.Empty:
            pass
        self.after(100, self._poll_logs)

    def _append_log(self, record: logging.LogRecord) -> None:
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%H:%M:%S")
        line = formatter.format(record) + "\n"
        tag = record.levelname

        self._log_box.configure(state="normal")
        self._log_box._textbox.insert("end", line, tag)
        self._log_box._textbox.see("end")
        self._log_box.configure(state="disabled")

    def _clear_logs(self) -> None:
        self._log_box.configure(state="normal")
        self._log_box.delete("0.0", "end")
        self._log_box.configure(state="disabled")

    # ------------------------------------------------------------------
    # State polling
    # ------------------------------------------------------------------

    def _poll_state(self) -> None:
        self._refresh_job_panel()
        self.after(500, self._poll_state)

    # ------------------------------------------------------------------
    # Server toggle
    # ------------------------------------------------------------------

    def _toggle_server(self) -> None:
        self._server_running = not self._server_running
        self._on_toggle_server(self._server_running)
        if self._server_running:
            self._toggle_btn.configure(text="Stop Server")
            self._set_status("idle")
        else:
            self._toggle_btn.configure(text="Start Server")
            self._set_status("stopped")

    def set_server_running(self, running: bool) -> None:
        self._server_running = running
        if running:
            self._toggle_btn.configure(text="Stop Server")
        else:
            self._toggle_btn.configure(text="Start Server")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _info_row(self, parent, label: str, value: str) -> ctk.CTkLabel:
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=1)
        ctk.CTkLabel(row, text=f"{label}:", width=80, anchor="w",
                     text_color="#aaaaaa").pack(side="left")
        val_lbl = ctk.CTkLabel(row, text=value, anchor="w")
        val_lbl.pack(side="left")
        return val_lbl

    def _on_close(self) -> None:
        self.destroy()
