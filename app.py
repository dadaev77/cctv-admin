# -*- coding: utf-8 -*-
import os
import csv
import io
import json
import sqlite3
import socket
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, Form, Depends, status, WebSocket, WebSocketDisconnect
import paramiko
from fastapi.responses import HTMLResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
import httpx

app = FastAPI(title="CCTV Manager - Панель управления")
TEMPLATES_DIR = os.environ.get("TEMPLATES_DIR", "/opt/cctv-admin/templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
DB_PATH = os.environ.get("DB_PATH", "/opt/cctv-admin/cctv.db")
MEDIAMTX_CONFIG_PATH = os.environ.get("MEDIAMTX_CONFIG_PATH", "/opt/mediamtx/mediamtx.yml")
GOLDEN_BACKUP_PATH = os.environ.get("GOLDEN_BACKUP_PATH", "/root/backup_golden_router.tar.gz")
SERVER_IP = "135.106.221.242"
MEDIAMTX_API = "http://127.0.0.1:9997"
RTSP_PORT = 8885
HLS_PORT = 8888
ADMIN_USER = "admin"
ADMIN_PASS = "admin123"

class NotAuthenticatedException(Exception):
    pass

@app.exception_handler(NotAuthenticatedException)
def auth_exception_handler(request: Request, exc: NotAuthenticatedException):
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS sites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        address TEXT,
        notes TEXT,
        created_at TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS routers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER,
        name TEXT NOT NULL,
        tunnel_port INTEGER UNIQUE,
        model TEXT DEFAULT 'Cudy LT300',
        notes TEXT,
        is_online INTEGER DEFAULT 0,
        last_seen TEXT,
        created_at TEXT,
        FOREIGN KEY(site_id) REFERENCES sites(id)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS cameras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER,
        router_id INTEGER,
        name TEXT NOT NULL,
        slug TEXT UNIQUE,
        cam_ip TEXT DEFAULT '192.168.1.213',
        cam_port INTEGER DEFAULT 554,
        username TEXT DEFAULT 'admin',
        password TEXT DEFAULT 'megatek123',
        channel TEXT DEFAULT '/Streaming/Channels/101',
        source_url TEXT,
        rtsp_public TEXT,
        hls_public TEXT,
        status TEXT DEFAULT 'offline',
        created_at TEXT,
        FOREIGN KEY(site_id) REFERENCES sites(id),
        FOREIGN KEY(router_id) REFERENCES routers(id)
    )""")
    
    # Автоматическая миграция колонок на случай старой базы
    r_cols = [row[1] for row in c.execute("PRAGMA table_info(routers)").fetchall()]
    if "tunnel_port" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN tunnel_port INTEGER DEFAULT 1554")
    if "model" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN model TEXT DEFAULT 'Cudy LT300'")
    if "notes" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN notes TEXT")
    if "created_at" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN created_at TEXT")
    if "uplink_type" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN uplink_type TEXT DEFAULT 'lte'")
    if "wifi_ssid" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN wifi_ssid TEXT")
    if "wifi_password" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN wifi_password TEXT")
    if "ssh_password" not in r_cols:
        c.execute("ALTER TABLE routers ADD COLUMN ssh_password TEXT DEFAULT ''")

    s_cols = [row[1] for row in c.execute("PRAGMA table_info(sites)").fetchall()]
    if "notes" not in s_cols:
        c.execute("ALTER TABLE sites ADD COLUMN notes TEXT")
        
    c_cols = [row[1] for row in c.execute("PRAGMA table_info(cameras)").fetchall()]
    if "cam_ip" not in c_cols:
        c.execute("ALTER TABLE cameras ADD COLUMN cam_ip TEXT DEFAULT '192.168.1.213'")
    if "cam_port" not in c_cols:
        c.execute("ALTER TABLE cameras ADD COLUMN cam_port INTEGER DEFAULT 554")
    if "username" not in c_cols:
        c.execute("ALTER TABLE cameras ADD COLUMN username TEXT DEFAULT 'admin'")
    if "password" not in c_cols:
        c.execute("ALTER TABLE cameras ADD COLUMN password TEXT DEFAULT 'megatek123'")
    if "channel" not in c_cols:
        c.execute("ALTER TABLE cameras ADD COLUMN channel TEXT DEFAULT '/Streaming/Channels/101'")
    
    # Заполнение первоначальными данными если пусто
    c.execute("SELECT COUNT(*) as cnt FROM sites")
    if c.fetchone()["cnt"] == 0:
        c.execute("INSERT INTO sites (name, address, notes, created_at) VALUES ('Объект №1 (Основной)', 'Центральный пост охраны', 'Тестовая камера', datetime('now', 'localtime'))")
        site_id = c.lastrowid
        c.execute("""INSERT INTO routers (site_id, name, tunnel_port, model, notes, is_online, last_seen, created_at) 
                     VALUES (?, 'Роутер Cudy 4G №1', 1554, 'Cudy LT300', 'Первый роутер с обратным туннелем', 1, datetime('now', 'localtime'), datetime('now', 'localtime'))""", (site_id,))
        router_id = c.lastrowid
        source = "rtsp://admin:megatek123@127.0.0.1:1554/Streaming/Channels/101"
        rtsp_pub = f"rtsp://{SERVER_IP}:{RTSP_PORT}/cam1"
        hls_pub = f"http://{SERVER_IP}:{HLS_PORT}/cam1"
        c.execute("""INSERT INTO cameras (site_id, router_id, name, slug, cam_ip, cam_port, username, password, channel, source_url, rtsp_public, hls_public, status, created_at)
                     VALUES (?, ?, 'Камера 1 (Шлагбаум)', 'cam1', '192.168.1.213', 554, 'admin', 'megatek123', '/Streaming/Channels/101', ?, ?, ?, 'online', datetime('now', 'localtime'))""",
                     (site_id, router_id, source, rtsp_pub, hls_pub))
    conn.commit()
    conn.close()

init_db()

def check_auth(request: Request):
    user = request.cookies.get("cctv_auth")
    if user != "authenticated":
        raise NotAuthenticatedException()

def is_port_open(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            return s.connect_ex(('127.0.0.1', port)) == 0
    except Exception:
        return False

async def sync_cam_to_mediamtx(slug: str, source_url: str):
    try:
        async with httpx.AsyncClient() as client:
            payload = {
                "source": source_url,
                "sourceProtocol": "tcp",
                "sourceOnDemand": True
            }
            res = await client.post(f"{MEDIAMTX_API}/v3/config/paths/add/{slug}", json=payload, timeout=2.0)
            if res.status_code != 200:
                await client.patch(f"{MEDIAMTX_API}/v3/config/paths/patch/{slug}", json=payload, timeout=2.0)
    except Exception:
        pass

async def remove_cam_from_mediamtx(slug: str):
    try:
        async with httpx.AsyncClient() as client:
            await client.delete(f"{MEDIAMTX_API}/v3/config/paths/delete/{slug}", timeout=2.0)
    except Exception:
        pass

def rewrite_mediamtx_yaml():
    try:
        conn = get_db()
        cams = conn.execute("SELECT slug, source_url FROM cameras").fetchall()
        conn.close()
        
        lines = [
            f"rtspAddress: :{RTSP_PORT}",
            "protocols: [tcp]",
            "hlsAlwaysRemux: yes",
            "paths:"
        ]
        for c in cams:
            if c['slug'] and c['source_url']:
                lines.append(f"  {c['slug']}:")
                lines.append(f"    source: {c['source_url']}")
                lines.append("    sourceOnDemand: yes")
            
        with open(MEDIAMTX_CONFIG_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return True
    except Exception as e:
        print("Error rewriting mediamtx.yml:", e)
        return False

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        resp = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
        resp.set_cookie("cctv_auth", "authenticated", max_age=86400*30)
        return resp
    return templates.TemplateResponse(request=request, name="login.html", context={"error": "Неверный логин или пароль"})

@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    resp.delete_cookie("cctv_auth")
    return resp

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, user: None = Depends(check_auth)):
    conn = get_db()
    sites = conn.execute("""
        SELECT s.*, 
               COUNT(DISTINCT r.id) as router_count, 
               COUNT(DISTINCT c.id) as camera_count 
        FROM sites s 
        LEFT JOIN routers r ON r.site_id = s.id 
        LEFT JOIN cameras c ON c.site_id = s.id 
        GROUP BY s.id 
        ORDER BY s.id ASC
    """).fetchall()
    routers = conn.execute("SELECT r.*, s.name as site_name FROM routers r LEFT JOIN sites s ON r.site_id=s.id ORDER BY r.id ASC").fetchall()
    cameras = conn.execute("SELECT c.*, s.name as site_name, r.name as router_name, r.tunnel_port FROM cameras c LEFT JOIN sites s ON c.site_id=s.id LEFT JOIN routers r ON c.router_id=r.id ORDER BY c.id ASC").fetchall()
    
    online_routers_count = sum(1 for r in routers if r["is_online"] == 1)

    # Suggest next free tunnel port
    used_ports = [r["tunnel_port"] for r in routers if r["tunnel_port"]]
    next_port = 1551
    for p in range(1551, 1600):
        if p not in used_ports:
            next_port = p
            break
            
    # Suggest next slug
    used_slugs = [c["slug"] for c in cameras if c["slug"]]
    next_slug = "cam1"
    for i in range(1, 100):
        test_slug = f"cam{i}"
        if test_slug not in used_slugs:
            next_slug = test_slug
            break

    conn.close()
    return templates.TemplateResponse(request=request, name="index.html", context={
        "sites": sites, 
        "routers": routers, 
        "cameras": cameras, 
        "online_routers_count": online_routers_count,
        "server_ip": SERVER_IP,
        "rtsp_port": RTSP_PORT,
        "hls_port": HLS_PORT,
        "next_port": next_port,
        "next_slug": next_slug
    })

# --- ОБЪЕКТЫ (SITES) ---
@app.post("/sites/add")
def add_site(name: str = Form(...), address: str = Form(""), notes: str = Form(""), user: None = Depends(check_auth)):
    conn = get_db()
    conn.execute("INSERT INTO sites (name, address, notes, created_at) VALUES (?, ?, ?, datetime('now', 'localtime'))", (name, address, notes))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/sites/{site_id}/edit")
def edit_site(site_id: int, name: str = Form(...), address: str = Form(""), notes: str = Form(""), user: None = Depends(check_auth)):
    conn = get_db()
    conn.execute("UPDATE sites SET name=?, address=?, notes=? WHERE id=?", (name, address, notes, site_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/sites/{site_id}/delete")
async def delete_site(site_id: int, user: None = Depends(check_auth)):
    conn = get_db()
    cams = conn.execute("SELECT slug FROM cameras WHERE site_id=?", (site_id,)).fetchall()
    for c in cams:
        if c["slug"]:
            await remove_cam_from_mediamtx(c["slug"])
    conn.execute("DELETE FROM cameras WHERE site_id=?", (site_id,))
    conn.execute("DELETE FROM routers WHERE site_id=?", (site_id,))
    conn.execute("DELETE FROM sites WHERE id=?", (site_id,))
    conn.commit()
    conn.close()
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

# --- РОУТЕРЫ (ROUTERS) ---
@app.post("/routers/add")
def add_router(
    site_id: int = Form(...), 
    name: str = Form(...), 
    tunnel_port: int = Form(...), 
    model: str = Form("Cudy LT300"), 
    uplink_type: str = Form("lte"),
    wifi_ssid: str = Form(""),
    wifi_password: str = Form(""),
    ssh_password: str = Form(""),
    notes: str = Form(""), 
    user: None = Depends(check_auth)
):
    conn = get_db()
    online = 1 if is_port_open(tunnel_port) else 0
    conn.execute("""INSERT INTO routers (site_id, name, tunnel_port, model, uplink_type, wifi_ssid, wifi_password, ssh_password, notes, is_online, last_seen, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))""",
                    (site_id, name, tunnel_port, model, uplink_type, wifi_ssid, wifi_password, ssh_password, notes, online))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/routers/{router_id}/edit")
def edit_router(
    router_id: int, 
    site_id: int = Form(...), 
    name: str = Form(...), 
    tunnel_port: int = Form(...), 
    model: str = Form("Cudy LT300"), 
    uplink_type: str = Form("lte"),
    wifi_ssid: str = Form(""),
    wifi_password: str = Form(""),
    ssh_password: str = Form(""),
    notes: str = Form(""), 
    user: None = Depends(check_auth)
):
    conn = get_db()
    conn.execute("""UPDATE routers SET site_id=?, name=?, tunnel_port=?, model=?, uplink_type=?, wifi_ssid=?, wifi_password=?, ssh_password=?, notes=? WHERE id=?""",
                 (site_id, name, tunnel_port, model, uplink_type, wifi_ssid, wifi_password, ssh_password, notes, router_id))
    
    # Update linked cameras source_url
    cams = conn.execute("SELECT id, username, password, cam_port, channel FROM cameras WHERE router_id=?", (router_id,)).fetchall()
    for c in cams:
        new_source = f"rtsp://{c['username']}:{c['password']}@127.0.0.1:{tunnel_port}{c['channel']}"
        conn.execute("UPDATE cameras SET source_url=? WHERE id=?", (new_source, c["id"]))
    
    conn.commit()
    conn.close()
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/routers/{router_id}/delete")
async def delete_router(router_id: int, user: None = Depends(check_auth)):
    conn = get_db()
    cams = conn.execute("SELECT slug FROM cameras WHERE router_id=?", (router_id,)).fetchall()
    for c in cams:
        if c["slug"]:
            await remove_cam_from_mediamtx(c["slug"])
    conn.execute("DELETE FROM cameras WHERE router_id=?", (router_id,))
    conn.execute("DELETE FROM routers WHERE id=?", (router_id,))
    conn.commit()
    conn.close()
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.get("/routers/download-backup")
def download_golden_backup(user: None = Depends(check_auth)):
    if os.path.exists(GOLDEN_BACKUP_PATH):
        return FileResponse(
            GOLDEN_BACKUP_PATH, 
            filename="backup_golden_router.tar.gz",
            media_type="application/gzip"
        )
    raise HTTPException(status_code=404, detail="Файл бэкапа не найден на сервере")

# --- КАМЕРЫ (CAMERAS) ---
@app.post("/cameras/add")
async def add_camera(
    site_id: int = Form(...),
    router_id: int = Form(...),
    name: str = Form(...),
    slug: str = Form(...),
    cam_ip: str = Form("192.168.1.213"),
    cam_port: int = Form(554),
    username: str = Form("admin"),
    password: str = Form("megatek123"),
    channel: str = Form("/Streaming/Channels/101"),
    user: None = Depends(check_auth)
):
    conn = get_db()
    r = conn.execute("SELECT tunnel_port, is_online FROM routers WHERE id=?", (router_id,)).fetchone()
    tunnel_port = r["tunnel_port"] if r and r["tunnel_port"] else 1554
    source_url = f"rtsp://{username}:{password}@127.0.0.1:{tunnel_port}{channel}"
    rtsp_pub = f"rtsp://{SERVER_IP}:{RTSP_PORT}/{slug}"
    hls_pub = f"http://{SERVER_IP}:{HLS_PORT}/{slug}"
    cam_status = "online" if r and r["is_online"] == 1 else "offline"

    conn.execute("""INSERT INTO cameras (site_id, router_id, name, slug, cam_ip, cam_port, username, password, channel, source_url, rtsp_public, hls_public, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))""",
                 (site_id, router_id, name, slug, cam_ip, cam_port, username, password, channel, source_url, rtsp_pub, hls_pub, cam_status))
    conn.commit()
    conn.close()

    await sync_cam_to_mediamtx(slug, source_url)
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/cameras/{camera_id}/edit")
async def edit_camera(
    camera_id: int,
    site_id: int = Form(...),
    router_id: int = Form(...),
    name: str = Form(...),
    slug: str = Form(...),
    cam_ip: str = Form("192.168.1.213"),
    cam_port: int = Form(554),
    username: str = Form("admin"),
    password: str = Form("megatek123"),
    channel: str = Form("/Streaming/Channels/101"),
    user: None = Depends(check_auth)
):
    conn = get_db()
    old_cam = conn.execute("SELECT slug FROM cameras WHERE id=?", (camera_id,)).fetchone()
    r = conn.execute("SELECT tunnel_port, is_online FROM routers WHERE id=?", (router_id,)).fetchone()
    tunnel_port = r["tunnel_port"] if r and r["tunnel_port"] else 1554
    source_url = f"rtsp://{username}:{password}@127.0.0.1:{tunnel_port}{channel}"
    rtsp_pub = f"rtsp://{SERVER_IP}:{RTSP_PORT}/{slug}"
    hls_pub = f"http://{SERVER_IP}:{HLS_PORT}/{slug}"
    cam_status = "online" if r and r["is_online"] == 1 else "offline"

    conn.execute("""UPDATE cameras SET site_id=?, router_id=?, name=?, slug=?, cam_ip=?, cam_port=?, username=?, password=?, channel=?, source_url=?, rtsp_public=?, hls_public=?, status=? WHERE id=?""",
                 (site_id, router_id, name, slug, cam_ip, cam_port, username, password, channel, source_url, rtsp_pub, hls_pub, cam_status, camera_id))
    conn.commit()
    conn.close()

    if old_cam and old_cam["slug"] != slug:
        await remove_cam_from_mediamtx(old_cam["slug"])
    await sync_cam_to_mediamtx(slug, source_url)
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/cameras/{camera_id}/delete")
async def delete_camera(camera_id: int, user: None = Depends(check_auth)):
    conn = get_db()
    cam = conn.execute("SELECT slug FROM cameras WHERE id=?", (camera_id,)).fetchone()
    if cam:
        slug = cam["slug"]
        conn.execute("DELETE FROM cameras WHERE id=?", (camera_id,))
        conn.commit()
        if slug:
            await remove_cam_from_mediamtx(slug)
    conn.close()
    rewrite_mediamtx_yaml()
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

@app.post("/sync-mediamtx")
async def sync_all_mediamtx(user: None = Depends(check_auth)):
    rewrite_mediamtx_yaml()
    conn = get_db()
    cams = conn.execute("SELECT slug, source_url FROM cameras").fetchall()
    conn.close()
    for c in cams:
        if c["slug"] and c["source_url"]:
            await sync_cam_to_mediamtx(c["slug"], c["source_url"])
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

# --- ЭКСПОРТ (ВЫГРУЗКА) ---
@app.get("/export/csv")
def export_csv(user: None = Depends(check_auth)):
    conn = get_db()
    query = """
        SELECT c.id, s.name as site_name, s.address, r.name as router_name, r.tunnel_port, r.uplink_type, r.wifi_ssid,
               c.name as cam_name, c.slug, c.source_url, c.rtsp_public, c.hls_public, c.status
        FROM cameras c
        LEFT JOIN sites s ON c.site_id = s.id
        LEFT JOIN routers r ON c.router_id = r.id
        ORDER BY c.id ASC
    """
    rows = conn.execute(query).fetchall()
    conn.close()

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["ID", "Объект", "Адрес", "Роутер", "Порт туннеля", "Тип связи", "Wi-Fi сеть", "Камера", "Slug", "RTSP ссылка", "HLS ссылка", "Статус"])
    
    for r in rows:
        writer.writerow([
            r["id"], r["site_name"] or "", r["address"] or "", r["router_name"] or "", 
            r["tunnel_port"] or "", r["uplink_type"] or "lte", r["wifi_ssid"] or "", r["cam_name"], r["slug"], r["rtsp_public"], r["hls_public"], r["status"]
        ])
        
    response = Response(content=output.getvalue(), media_type="text/csv; charset=utf-8")
    response.headers["Content-Disposition"] = "attachment; filename=cctv_cameras_export.csv"
    return response

@app.get("/export/m3u")
def export_m3u(user: None = Depends(check_auth)):
    conn = get_db()
    query = """
        SELECT c.id, s.name as site_name, c.name as cam_name, c.slug, c.rtsp_public
        FROM cameras c
        LEFT JOIN sites s ON c.site_id = s.id
        ORDER BY c.id ASC
    """
    rows = conn.execute(query).fetchall()
    conn.close()

    lines = ["#EXTM3U"]
    for r in rows:
        group = r["site_name"] or "Камеры"
        name = f"{r['cam_name']} ({group})"
        lines.append(f'#EXTINF:-1 tvg-id="{r["slug"]}" group-title="{group}",{name}')
        lines.append(r["rtsp_public"] or "")

    content = "\n".join(lines) + "\n"
    response = Response(content=content, media_type="application/x-mpegurl; charset=utf-8")
    response.headers["Content-Disposition"] = "attachment; filename=cameras_playlist.m3u"
    return response

@app.get("/export/json")
def export_json(user: None = Depends(check_auth)):
    conn = get_db()
    sites = [dict(r) for r in conn.execute("SELECT * FROM sites").fetchall()]
    routers = [dict(r) for r in conn.execute("SELECT * FROM routers").fetchall()]
    cameras = [dict(r) for r in conn.execute("SELECT * FROM cameras").fetchall()]
    conn.close()

    data = {
        "server_ip": SERVER_IP,
        "exported_at": datetime.now().isoformat(),
        "sites": sites,
        "routers": routers,
        "cameras": cameras
    }
    response = Response(content=json.dumps(data, ensure_ascii=False, indent=2), media_type="application/json; charset=utf-8")
    response.headers["Content-Disposition"] = "attachment; filename=cctv_backup_data.json"
    return response

# --- ФОНОВАЯ ПРОВЕРКА СТАТУСА ТУННЕЛЕЙ ---
async def background_checker():
    while True:
        try:
            conn = get_db()
            routers = conn.execute("SELECT id, tunnel_port FROM routers").fetchall()
            for r in routers:
                if r["tunnel_port"]:
                    online = 1 if is_port_open(r["tunnel_port"]) else 0
                    if online:
                        conn.execute("UPDATE routers SET is_online=1, last_seen=datetime('now', 'localtime') WHERE id=?", (r["id"],))
                    else:
                        conn.execute("UPDATE routers SET is_online=0 WHERE id=?", (r["id"],))
            
            conn.execute("""
                UPDATE cameras SET status = (
                    SELECT CASE WHEN r.is_online = 1 THEN 'online' ELSE 'offline' END
                    FROM routers r WHERE r.id = cameras.router_id
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            pass
        await asyncio.sleep(5)


# --- WEB SSH TERMINAL WEBSOCKET ---
@app.websocket("/ws/terminal/{router_id}")
async def terminal_ws(websocket: WebSocket, router_id: int, pwd: str = ""):
    cookie = websocket.cookies.get("cctv_auth")
    if cookie != "authenticated":
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()

    conn = get_db()
    r = conn.execute("SELECT tunnel_port, name, ssh_password FROM routers WHERE id=?", (router_id,)).fetchone()
    conn.close()

    if not r or not r["tunnel_port"]:
        await websocket.send_text("\r\n\x1b[31m[Ошибка] Роутер не найден в базе данных.\x1b[0m\r\n")
        await websocket.close()
        return

    ssh_port = r["tunnel_port"] + 100
    if not is_port_open(ssh_port):
        await websocket.send_text(
            f"\r\n\x1b[33m[Статус] SSH-порт {ssh_port} роутера '{r['name']}' сейчас оффлайн.\x1b[0m\r\n"
            f"\x1b[37mУбедитесь, что роутер включен и в команду туннеля /etc/rc.local добавлен порт {ssh_port}:\x1b[0m\r\n"
            f"\x1b[36m-R 127.0.0.1:{ssh_port}:127.0.0.1:22\x1b[0m\r\n"
        )
        await websocket.close()
        return

    # Choose password
    saved_pwd = r["ssh_password"] if ("ssh_password" in r.keys() and r["ssh_password"]) else ""
    target_pwd = pwd if pwd else saved_pwd

    await websocket.send_text(f"\x1b[32m[Подключение] Соединение с SSH-консолью {r['name']} (порт {ssh_port})...\x1b[0m\r\n")

    try:
        transport = paramiko.Transport(("127.0.0.1", ssh_port))
        transport.connect()
    except Exception as e:
        await websocket.send_text(f"\r\n\x1b[31m[Ошибка подключения] Не удалось открыть TCP сокет: {e}\x1b[0m\r\n")
        await websocket.close()
        return

    authenticated = False
    # If no password is provided, try auth_none first (OpenWrt default when passwordless)
    if not target_pwd:
        try:
            transport.auth_none("root")
            authenticated = True
        except Exception:
            pass

    # If auth_none was not accepted or a password was provided, try auth_password
    if not authenticated:
        try:
            transport.auth_password("root", target_pwd)
            authenticated = True
        except paramiko.AuthenticationException:
            await websocket.send_text("\r\n\x1b[31m[Ошибка авторизации] Неверный пароль root.\x1b[0m\r\n\x1b[33mВведите правильный пароль в поле вверху и нажмите кнопку «Войти».\x1b[0m\r\n")
            transport.close()
            await websocket.close()
            return
        except Exception as e:
            await websocket.send_text(f"\r\n\x1b[31m[Ошибка авторизации] {e}\x1b[0m\r\n")
            transport.close()
            await websocket.close()
            return

    chan = transport.open_session()
    chan.get_pty(term='xterm', width=80, height=24)
    chan.invoke_shell()
    chan.setblocking(False)

    stop_event = asyncio.Event()

    async def ssh_to_ws():
        try:
            while not stop_event.is_set():
                if chan.recv_ready():
                    data = chan.recv(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
                else:
                    await asyncio.sleep(0.02)
        except Exception:
            pass
        finally:
            stop_event.set()

    async def ws_to_ssh():
        try:
            while not stop_event.is_set():
                msg = await websocket.receive_text()
                try:
                    payload = json.loads(msg)
                    if payload.get("type") == "input":
                        chan.send(payload.get("data", "").encode("utf-8"))
                    elif payload.get("type") == "resize":
                        cols = int(payload.get("cols", 80))
                        rows = int(payload.get("rows", 24))
                        chan.resize_pty(width=cols, height=rows)
                except Exception:
                    chan.send(msg.encode("utf-8"))
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            stop_event.set()

    tasks = [asyncio.create_task(ssh_to_ws()), asyncio.create_task(ws_to_ssh())]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    stop_event.set()
    for t in tasks:
        t.cancel()

    try:
        chan.close()
        transport.close()
    except Exception:
        pass

@app.on_event("startup")

async def on_startup():
    asyncio.create_task(background_checker())
