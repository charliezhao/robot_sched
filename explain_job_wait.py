#!/usr/bin/env python3
"""
CLI Tool: Explain why a specific job waited or failed to dispatch.
Usage: python explain_job_wait.py <JOB_ID>
"""
import sqlite3
import sys
from rich.console import Console
from rich.table import Table

DB_PATH = "scheduler_state.db"
console = Console()

def explain_job(job_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 1. Fetch Job Metadata
    job = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    if not job:
        console.print(f"[bold red]Job '{job_id}' not found in database ledger.[/]")
        return

    console.print(f"\n[bold green]═══ FORENSIC DISPATCH REPORT: {job_id} ═══[/]")
    console.print(f"Status: [bold cyan]{job['status']}[/] | WorkType: [yellow]{job['work_type']}[/] | State Req: [magenta]{job['required_state_hash']}[/]")
    console.print(f"Retry Count: {job['attempt_count']}/{job['retry_limit']} | Assigned Station: {job['assigned_station_id'] or 'None'}\n")

    # 2. Query Rejection Ledger
    cur = conn.execute("""
        SELECT sim_time, station_id, station_table_state, available_gpus, decision_verdict, rejection_reason
        FROM decision_audit_ledger
        WHERE candidate_job_id = ?
        ORDER BY sim_time ASC
    """, (job_id,))
    rows = cur.fetchall()

    if not rows:
        console.print("[dim]No evaluation traces logged for this job (may have been refused at preflight or unqueued).[/]")
        return

    # Aggregate rejection reasons
    reasons_summary = {}
    for r in rows:
        v = r["decision_verdict"]
        reasons_summary[v] = reasons_summary.get(v, 0) + 1

    console.print("[bold underline]Decision Verdict Breakdown:[/]")
    for r_type, count in reasons_summary.items():
        if r_type == "DISPATCHED":
            console.print(f"  • [bold green]{r_type:<28}[/]: {count} time(s)")
        else:
            console.print(f"  • [bold red]{r_type:<28}[/]: bypassed {count} time(s)")


    # Show chronological timeline sample
    t = Table(title="\nDetailed Decision Traversal Sample (First 15 Evaluation Cycles)", expand=True)
    t.add_column("Sim Time (s)", justify="right", style="cyan")
    t.add_column("Station", style="bold")
    t.add_column("Station Table State", style="yellow")
    t.add_column("Free GPUs", justify="center")
    t.add_column("Verdict", style="red")
    t.add_column("Specific Forensic Reason")

    for r in rows[:15]:
        verdict_style = "bold green" if r["decision_verdict"] == "DISPATCHED" else "bold red"
        t.add_row(
            f"{r['sim_time']:.1f}",
            r["station_id"],
            r["station_table_state"],
            str(r["available_gpus"]),
            f"[{verdict_style}]{r['decision_verdict']}[/]",
            r["rejection_reason"]
        )

    console.print(t)
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print("Usage: python explain_job_wait.py <job_id>")
        sys.exit(1)
    explain_job(sys.argv[1])