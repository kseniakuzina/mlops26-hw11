"""REPORT.md -> report.html (самодостаточный: графики встроены как data URI, печать на A4)."""
import base64
import re
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parent.parent
CSS = """
:root{--fg:#1f2328;--muted:#59636e;--bg:#fff;--line:#d1d9e0;--code:#f6f8fa;--accent:#0b6e4f}
@media (prefers-color-scheme: dark){:root{--fg:#e6edf3;--muted:#9198a1;--bg:#0d1117;--line:#3d444d;--code:#161b22;--accent:#3fb950}}
body{font:15px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;color:var(--fg);background:var(--bg);
max-width:1080px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:1.8em;border-bottom:2px solid var(--accent);padding-bottom:.2em}
h2{margin-top:2em;border-bottom:1px solid var(--line);padding-bottom:.2em}
h3{margin-top:1.4em}
table{border-collapse:collapse;margin:.8em 0;font-size:.88em;display:block;overflow-x:auto}
th,td{border:1px solid var(--line);padding:4px 8px;text-align:left;vertical-align:top}
th{background:var(--code)}
code{background:var(--code);padding:1px 4px;border-radius:4px;font-size:.9em}
pre{background:var(--code);padding:10px;border-radius:6px;overflow-x:auto;font-size:.85em}
pre code{padding:0}
img{max-width:100%;border:1px solid var(--line);border-radius:6px;background:#fff}
blockquote{border-left:4px solid var(--accent);margin:1em 0;padding:.2em 1em;color:var(--muted)}
.muted{color:var(--muted)}
@media print{body{max-width:none;font-size:11pt}h2{page-break-before:auto}img{page-break-inside:avoid}}
"""


def inline_images(html):
    def repl(m):
        path = ROOT / m.group(1)
        if not path.exists():
            return m.group(0)
        data = base64.b64encode(path.read_bytes()).decode()
        return f'src="data:image/png;base64,{data}"'
    return re.sub(r'src="([^"]+\.png)"', repl, html)


def main():
    md = (ROOT / 'REPORT.md').read_text(encoding='utf-8')
    body = markdown.markdown(md, extensions=['tables', 'fenced_code', 'toc', 'sane_lists'])
    title = 'ДЗ 11. Kubernetes под нагрузкой — отчёт'
    html = (f'<!doctype html><html lang="ru"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1"><title>{title}</title>'
            f'<style>{CSS}</style></head><body>{inline_images(body)}</body></html>\n')
    (ROOT / 'report.html').write_text(html, encoding='utf-8')
    print(ROOT / 'report.html', len(html) // 1024, 'KiB')


if __name__ == '__main__':
    main()
