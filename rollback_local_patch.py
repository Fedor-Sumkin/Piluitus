# -*- coding: utf-8 -*-
import argparse, json, os, shutil, tempfile
from pathlib import Path

def restore(src, dst):
    dst.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=str(dst.parent),prefix=dst.name+".",suffix=".tmp")
    try:
        with os.fdopen(fd,"wb") as out, src.open("rb") as inp: shutil.copyfileobj(inp,out)
        os.replace(tmp,dst)
    except Exception:
        try: os.unlink(tmp)
        except OSError: pass
        raise

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--project",default="."); project=Path(ap.parse_args().project).resolve()
    backups=sorted((p for p in project.glob("_backup_local_patch_*") if (p/"manifest.json").is_file()),key=lambda p:p.name,reverse=True)
    if not backups: print("Резервная копия не найдена."); return 2
    backup=backups[0]; data=json.loads((backup/"manifest.json").read_text(encoding="utf-8"))
    for item in data["files"]:
        dst=project/item["path"]; src=backup/item["path"]
        if item["existed_before"]:
            if not src.is_file(): print("Не найден резервный файл:",src); return 3
            restore(src,dst)
        elif dst.exists(): dst.unlink()
    print("Откат выполнен из",backup); return 0

if __name__ == "__main__": raise SystemExit(main())
