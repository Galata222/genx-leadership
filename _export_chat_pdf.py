"""Render the Cursor agent transcript to HTML, then print it to PDF."""

import html
import json
import re
import subprocess
from pathlib import Path

TRANSCRIPT = Path(
    r"C:\Users\Admin\.cursor\projects\c-Users-Admin-Desktop-fastapi-webhook-assessment"
    r"\agent-transcripts\06710832-a687-496b-b117-4174484c9886"
    r"\06710832-a687-496b-b117-4174484c9886.jsonl"
)
HTML_PATH = Path(r"C:\Users\Admin\Desktop\fastapi-webhook-assessment\AI_CHAT_HISTORY.html")
PDF_PATH = Path(r"C:\Users\Admin\Desktop\fastapi-webhook-assessment\AI_CHAT_HISTORY.pdf")
CHROME = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")

EXTRA_USER = (
    "please export the chat pdf"
)


def clean_user_text(text: str) -> str:
    text = re.sub(
        r"<timestamp>\s*(.*?)\s*</timestamp>\s*",
        r"\1\n",
        text,
        flags=re.DOTALL,
    )
    text = text.replace("<user_query>", "").replace("</user_query>", "")
    return text.strip()


def blocks(record: dict) -> list[dict]:
    message = record.get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if isinstance(content, list):
        return content
    return []


def render_tool(block: dict) -> str:
    name = html.escape(str(block.get("name") or "tool"))
    payload = block.get("input")
    if payload is None:
        payload = block.get("arguments")
    try:
        body = json.dumps(payload, indent=2, ensure_ascii=False)
    except TypeError:
        body = str(payload)
    return (
        f'<div class="tool"><div class="tool-name">Tool: {name}</div>'
        f"<pre>{html.escape(body)}</pre></div>"
    )


def main() -> None:
    records = [
        json.loads(line)
        for line in TRANSCRIPT.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    user_texts = []
    for record in records:
        if record.get("role") != "user":
            continue
        for block in blocks(record):
            if block.get("type") == "text":
                user_texts.append(clean_user_text(block.get("text") or ""))
    if EXTRA_USER not in "\n".join(user_texts):
        records.append(
            {
                "role": "user",
                "message": {"content": [{"type": "text", "text": EXTRA_USER}]},
            }
        )

    parts: list[str] = []
    turn = 0
    for record in records:
        role = record.get("role") or "unknown"
        turn += 1
        label = "You" if role == "user" else "AI agent" if role == "assistant" else role
        chunks: list[str] = []
        for block in blocks(record):
            kind = block.get("type")
            if kind == "text":
                text = block.get("text") or ""
                if role == "user":
                    text = clean_user_text(text)
                if text:
                    chunks.append(f"<p>{html.escape(text)}</p>")
            elif kind == "tool_use":
                chunks.append(render_tool(block))
        if not chunks:
            continue
        parts.append(
            f'<article class="msg {html.escape(role)}">'
            f'<header><span class="who">{html.escape(label)}</span>'
            f'<span class="n">#{turn}</span></header>'
            + "\n".join(chunks)
            + "</article>"
        )

    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>AI chat history — webhook ingestion assessment</title>
<style>
  @page {{ size: A4; margin: 14mm 12mm 16mm 12mm; }}
  body {{
    font-family: "Segoe UI", Calibri, sans-serif;
    font-size: 11pt;
    line-height: 1.45;
    color: #1c1c1c;
  }}
  h1 {{ font-size: 18pt; margin: 0 0 4px; }}
  .meta {{ color: #444; margin: 0 0 18px; font-size: 10pt; }}
  article {{
    border: 1px solid #d0d0d0;
    border-left-width: 4px;
    padding: 8px 12px 4px;
    margin: 0 0 12px;
    break-inside: auto;
  }}
  article.user {{ border-left-color: #1d4e89; background: #f4f8fc; }}
  article.assistant {{ border-left-color: #2f6f4e; background: #fff; }}
  header {{ font-size: 10pt; margin-bottom: 6px; }}
  .who {{ font-weight: 700; }}
  .n {{ float: right; color: #777; }}
  p {{ white-space: pre-wrap; margin: 0 0 8px; }}
  .tool {{
    background: #f6f6f6;
    border: 1px solid #e2e2e2;
    margin: 0 0 8px;
    padding: 6px 8px;
  }}
  .tool-name {{ font-weight: 700; font-size: 9.5pt; margin-bottom: 4px; }}
  pre {{
    font-family: Consolas, "Cascadia Mono", monospace;
    font-size: 8pt;
    line-height: 1.35;
    white-space: pre-wrap;
    word-break: break-word;
    margin: 0;
  }}
</style>
</head>
<body>
<h1>AI chat history</h1>
<p class="meta">Webhook ingestion service — backend technical assessment.<br>
Exported from the Cursor agent transcript for this project. Tool calls are included so the history shows the code the agent wrote and the commands it ran.</p>
{"".join(parts)}
</body>
</html>
"""
    HTML_PATH.write_text(document, encoding="utf-8")
    if PDF_PATH.exists():
        PDF_PATH.unlink()
    subprocess.run(
        [
            str(CHROME),
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--no-pdf-header-footer",
            f"--print-to-pdf={PDF_PATH}",
            HTML_PATH.as_uri(),
        ],
        check=True,
        timeout=120,
    )
    print(f"html_bytes={HTML_PATH.stat().st_size}")
    print(f"pdf_bytes={PDF_PATH.stat().st_size}")
    print(f"pdf={PDF_PATH}")


if __name__ == "__main__":
    main()
