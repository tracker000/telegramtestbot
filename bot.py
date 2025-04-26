from __future__ import annotations
import asyncio, hashlib, html, logging, os, re, sqlite3, textwrap, certifi, feedparser, openai, requests
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from logging.handlers import RotatingFileHandler
from typing import Any, Iterable, Sequence
from bs4 import BeautifulSoup
from dateutil import parser as dtparse
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import AIORateLimiter, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, JobQueue

FEED_URL="https://www.contractsfinder.service.gov.uk/Published/Notices/Rss"
DB_FILE,LOG_DIR,CACHE_DIR="bot.db","log","cache"
MAX_SUBS,FETCH_MIN,HTTP_RETRIES,HTTP_BACKOFF=5,10,3,2
OPENAI_RETRIES,OPENAI_LIMIT,MSG_MAX=3,60,4096
VALID_KEYWORD=re.compile(r"^[A-Za-z0-9_-]{2,30}$")
BOUND=lambda k:re.compile(rf"(?<![A-Za-z0-9_]){re.escape(k)}(?![A-Za-z0-9_])",re.IGNORECASE)
UI={"welcome":"Welcome! Use /subscribe <kw> … /help","help":"/start /subscribe /unsubscribe /list /clear",
     "no_kw":"Please specify at least one valid keyword.","sub_limit":f"Max {MAX_SUBS} keywords.",
     "already":"You are already registered.","subscribed":"Subscribed to: {added}.","none_added":"Nothing added.",
     "removed":"Removed: {kw}.","not_found":"Keyword(s) not found: {kw}.","subs":"Your keywords: {subs}.",
     "subs_empty":"You don’t have any keywords yet.","cleared":"All keywords removed.","fb_ok":"Feedback recorded ✅",
     "kw_invalid":"Keyword ‘{kw}’ invalid – skipped."}

load_dotenv()
BOT_TOKEN,OPENAI_API_KEY=os.getenv("BOT_TOKEN"),os.getenv("OPENAI_API_KEY")
if not BOT_TOKEN or not OPENAI_API_KEY:raise RuntimeError
openai.api_key=OPENAI_API_KEY
os.makedirs(LOG_DIR,exist_ok=True);os.makedirs(CACHE_DIR,exist_ok=True)
cut=datetime.now(UTC)-timedelta(days=30)
for f in os.scandir(CACHE_DIR):
    if f.is_file() and datetime.fromtimestamp(f.stat().st_mtime,UTC)<cut:os.unlink(f.path)

mask=lambda cid:hashlib.sha1(str(cid).encode()).hexdigest()[:6]
logger=logging.getLogger("tb");logger.setLevel(logging.INFO)
fh=RotatingFileHandler(os.path.join(LOG_DIR,"bot.log"),5_242_880,2)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"));logger.addHandler(fh);logger.addHandler(logging.StreamHandler())

_conn:sqlite3.Connection|None=None
_db_lock=asyncio.Lock()
def conn_get()->sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn=sqlite3.connect(DB_FILE,check_same_thread=False,timeout=30)
        _conn.execute("PRAGMA journal_mode=WAL")
    return _conn
@contextmanager
def cx()->Iterable[sqlite3.Connection]:
    c=conn_get()
    try:yield c
    finally:c.commit()
async def db_exec(sql:str,params:Sequence[Any]|None=None)->int:
    async with _db_lock:
        with cx() as c:
            cur=c.execute(sql,params or ());return cur.rowcount
async def db_query(sql:str,params:Sequence[Any]|None=None)->list[tuple]:
    async with _db_lock:
        with cx() as c:return c.execute(sql,params or ()).fetchall()
async def init_db():
    await db_exec("CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY, joined_at TEXT)")
    await db_exec("CREATE TABLE IF NOT EXISTS subs (chat_id INTEGER,keyword TEXT,last_seen TEXT,PRIMARY KEY(chat_id,keyword))")
    await db_exec("CREATE TABLE IF NOT EXISTS feedback (chat_id INTEGER,tender_id TEXT,feedback TEXT,timestamp TEXT)")
    await db_exec("CREATE TABLE IF NOT EXISTS sent (chat_id INTEGER,tender_id TEXT,PRIMARY KEY(chat_id,tender_id))")

_regex_cache:dict[str,re.Pattern[str]]={}
def parse_keywords(raw:str)->list[str]:
    out=[]
    for seg in re.split(r"[\s,]+",raw):
        kw=seg.strip().lower()
        if not kw:continue
        if not VALID_KEYWORD.fullmatch(kw):logger.warning(UI["kw_invalid"].format(kw=kw));continue
        out.append(kw)
    return out
def kw_match(txt:str,kw:str)->bool:
    pat=_regex_cache.get(kw)
    if pat is None:pat=_regex_cache.setdefault(kw,BOUND(kw))
    return bool(pat.search(txt))

async def fetch_feed()->list[dict[str,Any]]:
    for att in range(1,HTTP_RETRIES+1):
        try:
            resp=await asyncio.get_running_loop().run_in_executor(None,partial(requests.get,FEED_URL,timeout=10,headers={"User-Agent":"TenderBot/3.0"},verify=certifi.where()))
            resp.raise_for_status();break
        except Exception:
            if att==HTTP_RETRIES:logger.exception("RSS fail");raise
            await asyncio.sleep(HTTP_BACKOFF*2**(att-1))
    feed=feedparser.parse(resp.text)
    return[{"id":e.get("id")or e.get("link"),"title":e.get("title","").strip(),"link":e.get("link"),
            "summary":BeautifulSoup(e.get("summary",""),"html.parser").get_text(),
            "published":e.get("published"),"updated":e.get("updated")}for e in feed.entries]

_openai_calls:list[datetime]=[]
async def rate_limit():
    now=datetime.now(UTC);_openai_calls.append(now)
    while _openai_calls and (now-_openai_calls[0]).total_seconds()>60:_openai_calls.pop(0)
    if len(_openai_calls)>=OPENAI_LIMIT:
        delay=61-(now-_openai_calls[0]).total_seconds()
        logger.info("OpenAI sleep %.1f",delay);await asyncio.sleep(delay)
def cache_p(t:str)->str:return os.path.join(CACHE_DIR,hashlib.sha256(t.encode()).hexdigest()+".txt")
async def summarise(raw:str)->str:
    sn=raw.strip()[:1500];cp=cache_p(sn)
    if os.path.exists(cp):
        with open(cp,"r",encoding="utf-8")as f:return f.read()
    for at in range(1,OPENAI_RETRIES+1):
        try:
            await rate_limit()
            res=await asyncio.get_running_loop().run_in_executor(None,partial(openai.ChatCompletion.create,model="gpt-4o",timeout=15,messages=[{"role":"system","content":"Summarise in max 60 words."},{"role":"user","content":sn}]))
            sm=res.choices[0].message.content.strip();break
        except Exception:
            if at==OPENAI_RETRIES:sm="[⚠️ Summary unavailable] "+textwrap.shorten(sn,120)
            else:await asyncio.sleep(2*at)
    with open(cp,"w",encoding="utf-8")as f:f.write(sm)
    return sm

def build_msg(e:dict[str,Any],summary:str,kw:str,upd:bool)->str:
    t=e["title"]+(" (Updated)" if upd else "")
    body="\n".join([f"📝 <b>{html.escape(t)}</b>",html.escape(summary),f"🔍 <code>{html.escape(kw)}</code>"])
    link=f"🔗 <a href='{html.escape(e['link'])}'>View tender</a>"
    return f"{body}\n\n{link}"
def split_html(txt:str)->list[str]:
    if len(txt)<=MSG_MAX:return[txt]
    parts=[]
    while len(txt)>MSG_MAX:
        cut=max(txt.rfind("\n\n",0,MSG_MAX),txt.rfind("<p",0,MSG_MAX))
        if cut<=0:cut=MSG_MAX
        parts.append(txt[:cut]);txt=txt[cut:].lstrip()
    parts.append(txt);return parts
async def send_split(ctx:ContextTypes.DEFAULT_TYPE,cid:int,txt:str,mark:InlineKeyboardMarkup|None):
    for i,ch in enumerate(split_html(txt)):
        await ctx.bot.send_message(cid,ch,parse_mode=ParseMode.HTML,reply_markup=mark if i==0 else None,disable_web_page_preview=True)

async def cmd_start(u:Update,_):
    cid=u.effective_chat.id
    if not await db_query("SELECT 1 FROM users WHERE chat_id=?", (cid,)):
        await db_exec("INSERT INTO users VALUES (?,?)",(cid,datetime.now(UTC).isoformat(timespec="seconds")+"Z"))
        await u.message.reply_text(UI["welcome"])
    else:await u.message.reply_text(UI["already"])
async def cmd_help(u:Update,_):await u.message.reply_text(UI["help"])
async def cmd_subscribe(u:Update,_):
    cid=u.effective_chat.id;kws=parse_keywords(" ".join(u.message.text.split()[1:]))
    if not kws:return await u.message.reply_text(UI["no_kw"])
    cur={r[0] for r in await db_query("SELECT keyword FROM subs WHERE chat_id=?", (cid,))}
    if len(cur)>=MAX_SUBS:return await u.message.reply_text(UI["sub_limit"])
    added=[]
    for kw in kws:
        if len(cur)+len(added)>=MAX_SUBS:break
        if kw not in cur:
            await db_exec("INSERT INTO subs VALUES (?,?,?)",(cid,kw,"1970-01-01T00:00:00Z"));added.append(kw)
    await u.message.reply_text(UI["subscribed" if added else "none_added"].format(added=", ".join(added)))
async def cmd_unsubscribe(u:Update,_):
    cid=u.effective_chat.id;kws=parse_keywords(" ".join(u.message.text.split()[1:]))
    if not kws:return await u.message.reply_text(UI["no_kw"])
    removed=[]
    for kw in kws:
        if await db_exec("DELETE FROM subs WHERE chat_id=? AND keyword=?", (cid,kw))>0:removed.append(kw)
    await u.message.reply_text(UI["removed" if removed else "not_found"].format(kw=", ".join(removed or kws)))
async def cmd_list(u:Update,_):
    cid=u.effective_chat.id
    subs=[r[0] for r in await db_query("SELECT keyword FROM subs WHERE chat_id=?", (cid,))]
    await u.message.reply_text(UI["subs"].format(subs=", ".join(subs)) if subs else UI["subs_empty"])
async def cmd_clear(u:Update,_):
    cid=u.effective_chat.id;await db_exec("DELETE FROM subs WHERE chat_id=?", (cid,));await u.message.reply_text(UI["cleared"])

async def cb_buttons(u:Update,_):
    q=u.callback_query
    if not q or not q.data:return
    await q.answer(UI["fb_ok"],show_alert=False)
    code,tid=q.data.split(":",1)
    fb={"suit":"relevant","unsuit":"irrelevant"}.get(code,"?")
    await db_exec("INSERT INTO feedback VALUES (?,?,?,?)",(q.from_user.id,tid,fb,datetime.now(UTC).isoformat(timespec="seconds")+"Z"))
    try:await q.edit_message_reply_markup(reply_markup=None)
    except:pass

async def scan_feed_job(ctx:ContextTypes.DEFAULT_TYPE):
    if ctx.job.data.get("running"):return
    ctx.job.data["running"]=True
    try:
        entries=await fetch_feed()
        sent={(r[0],r[1]) for r in await db_query("SELECT chat_id,tender_id FROM sent")}
        subs=await db_query("SELECT chat_id,keyword,last_seen FROM subs")
        for cid,kw,last in subs:
            threshold=dtparse.isoparse(last).astimezone(UTC)
            for e in entries:
                if not kw_match(e["title"],kw) and not kw_match(e["summary"],kw):continue
                ts=e["updated"] or e["published"]
                if not ts:continue
                try:edt=dtparse.parse(ts).astimezone(UTC)
                except:edt=datetime.now(UTC)
                if edt<=threshold or (cid,e["id"]) in sent:continue
                summ=await summarise(e["summary"])
                msg=build_msg(e,summ,kw,bool(e["updated"]))
                kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Relevant",callback_data=f"suit:{e['id']}"),
                                           InlineKeyboardButton("❌ Irrelevant",callback_data=f"unsuit:{e['id']}")]])
                try:
                    await send_split(ctx,cid,msg,kb)
                    try:await db_exec("INSERT INTO sent VALUES (?,?)",(cid,e["id"]))
                    except sqlite3.IntegrityError:logger.debug("dup %s %s",mask(cid),e["id"])
                    await db_exec("UPDATE subs SET last_seen=? WHERE chat_id=? AND keyword=?",(edt.isoformat(timespec="seconds")+"Z",cid,kw))
                except:logger.exception("send %s %s",mask(cid),e["id"])
    finally:
        ctx.job.data["running"]=False

async def main():
    await init_db()
    app=ApplicationBuilder().token(BOT_TOKEN).rate_limiter(AIORateLimiter()).build()
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("help",cmd_help))
    app.add_handler(CommandHandler("subscribe",cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe",cmd_unsubscribe))
    app.add_handler(CommandHandler("list",cmd_list))
    app.add_handler(CommandHandler("clear",cmd_clear))
    app.add_handler(CallbackQueryHandler(cb_buttons))
    app.job_queue.run_repeating(scan_feed_job,interval=FETCH_MIN*60,first=5,coalesce=True,data={"running":False})
    logger.info("Bot started");await app.run_polling()

if __name__=="__main__":
    try:asyncio.run(main())
    except KeyboardInterrupt:logger.info("Bot stopped")
