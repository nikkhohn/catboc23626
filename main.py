import asyncio
import os
import re
import time
import logging
import aiohttp
import aiofiles
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("TeraBot")

# ─── Config ──────────────────────────────────────────────────────────────────
API_ID             = int(os.environ["API_ID"])
API_HASH           = os.environ["API_HASH"]
BOT_TOKEN          = os.environ["BOT_TOKEN"]
USER_SESSION       = os.environ["USER_SESSION"]
OWNER_ID           = int(os.environ["OWNER_ID"])

TERABOX_DOWNLOADER_BOT = "@TeraBoxDownloader_TgBot"

# TeraBox domains regex
TERABOX_RE = re.compile(
    r"https?://(?:www\.)?"
    r"(?:terabox\.com|teraboxapp\.com|teraboxlink\.com|freeterabox\.com|"
    r"4funbox\.com|1024tera\.com|mirrobox\.com|nephobox\.com|"
    r"momerybox\.com|tibibox\.com|gibibox\.com|dubox\.com|"
    r"1024terabox\.com|terabox\.fun|teraboxvideos\.com|terasharelink\.com)"
    r"/[^\s]+",
    re.IGNORECASE,
)

# ─── Shared State ────────────────────────────────────────────────────────────
@dataclass
class Job:
    chat_id:             int
    terabox_url:         str
    image_bytes:         Optional[bytes] = None
    image_filename:      str = "thumbnail.jpg"
    catbox_image_url:    Optional[str] = None
    catbox_video_url:    Optional[str] = None
    sent_msg_id:         Optional[int] = None   # msg id of link we sent to TeraBox bot
    created_at:          float = field(default_factory=time.time)

job_queue:    asyncio.Queue  = asyncio.Queue()

# sent_msg_id → Job  (for exact reply matching)
pending_jobs: dict[int, Job] = {}

processing_lock = asyncio.Lock()

# ─── Catbox Upload ────────────────────────────────────────────────────────────
async def upload_to_catbox(session: aiohttp.ClientSession, data: bytes, filename: str) -> str:
    form = aiohttp.FormData()
    form.add_field("reqtype",      "fileupload")
    form.add_field("userhash",     "")
    form.add_field("fileToUpload", data, filename=filename,
                   content_type="application/octet-stream")

    async with session.post(
        "https://catbox.moe/user/api.php",
        data=form,
        timeout=aiohttp.ClientTimeout(total=300),
    ) as resp:
        resp.raise_for_status()
        url = (await resp.text()).strip()
        if not url.startswith("https://"):
            raise ValueError(f"Catbox unexpected response: {url}")
        return url

# ─── Queue Worker ─────────────────────────────────────────────────────────────
async def queue_worker(bot: Client, userbot: Client):
    async with aiohttp.ClientSession() as session:
        while True:
            job: Job = await job_queue.get()
            try:
                async with processing_lock:
                    await process_job(bot, userbot, session, job)
            except Exception as e:
                log.exception(f"Job failed: {e}")
                try:
                    await bot.send_message(job.chat_id, f"❌ Job fail ho gaya:\n`{e}`")
                except Exception:
                    pass
            finally:
                job_queue.task_done()

async def process_job(bot: Client, userbot: Client,
                      session: aiohttp.ClientSession, job: Job):
    log.info(f"Processing: {job.terabox_url}")

    # Step 1: Image → Catbox
    await bot.send_message(job.chat_id, "⏳ Thumbnail Catbox pe upload ho raha hai...")
    job.catbox_image_url = await upload_to_catbox(
        session, job.image_bytes, job.image_filename
    )
    log.info(f"Image uploaded: {job.catbox_image_url}")

    # Step 2: Send TeraBox link via userbot (FloodWait handled)
    await bot.send_message(job.chat_id, "📤 TeraBox bot ko link bhej raha hun...")
    while True:
        try:
            sent = await userbot.send_message(TERABOX_DOWNLOADER_BOT, job.terabox_url)
            break
        except FloodWait as fw:
            log.warning(f"FloodWait {fw.value}s")
            await asyncio.sleep(fw.value + 2)

    job.sent_msg_id = sent.id
    pending_jobs[sent.id] = job
    log.info(f"Link sent, msg_id={sent.id}")

    # Step 3: Wait for video reply (8 min timeout)
    deadline = time.time() + 480
    while time.time() < deadline:
        if job.catbox_video_url is not None:
            break
        await asyncio.sleep(3)

    pending_jobs.pop(job.sent_msg_id, None)

    if job.catbox_video_url is None:
        raise TimeoutError("TeraBox bot ne 8 minute mein video nahi diya.")

    if job.catbox_video_url.startswith("ERROR:"):
        raise RuntimeError(job.catbox_video_url)

    # Step 4: Done — DM owner
    await bot.send_message(
        job.chat_id,
        f"✅ **Done!**\n\n"
        f"🖼 **Image (Catbox):**\n{job.catbox_image_url}\n\n"
        f"🎬 **Video (Catbox):**\n{job.catbox_video_url}",
    )
    log.info(f"Job complete: {job.terabox_url}")

# ─── Userbot: TeraBox bot reply monitor ──────────────────────────────────────
def attach_reply_monitor(userbot: Client):
    """
    TeraBox bot replies to our sent message with the video.
    We match via reply_to_message.id == job.sent_msg_id — 100% accurate.
    """

    @userbot.on_message(
        filters.user(TERABOX_DOWNLOADER_BOT)
        & (filters.video | filters.document)
    )
    async def terabox_reply_received(client: Client, message: Message):
        # Get the message id this is a reply to
        reply_to_id = (
            message.reply_to_message.id
            if message.reply_to_message
            else None
        )
        log.info(f"TeraBox bot sent video, reply_to={reply_to_id}")

        job = None
        if reply_to_id and reply_to_id in pending_jobs:
            job = pending_jobs[reply_to_id]
        elif pending_jobs:
            # Fallback: if no reply_to (some bots don't reply), take oldest
            oldest_id = min(pending_jobs.keys())
            job = pending_jobs[oldest_id]
            log.warning("No reply_to match, using oldest pending job as fallback.")

        if job is None:
            log.info("No matching job found, ignoring.")
            return

        log.info(f"Matched video to job: {job.terabox_url}")

        # Download video and upload to Catbox
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp_path = tmp.name

        try:
            await client.download_media(message, file_name=tmp_path)
            async with aiofiles.open(tmp_path, "rb") as f:
                video_bytes = await f.read()

            video_filename = "video.mp4"
            if message.video and message.video.file_name:
                video_filename = message.video.file_name
            elif message.document and message.document.file_name:
                video_filename = message.document.file_name

            async with aiohttp.ClientSession() as sess:
                job.catbox_video_url = await upload_to_catbox(sess, video_bytes, video_filename)

            log.info(f"Video uploaded: {job.catbox_video_url}")

        except Exception as e:
            log.exception(f"Video upload failed: {e}")
            job.catbox_video_url = f"ERROR: {e}"
        finally:
            Path(tmp_path).unlink(missing_ok=True)

# ─── Bot: forward handler ─────────────────────────────────────────────────────
def make_bot(userbot: Client) -> Client:
    bot = Client("terabot_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

    @bot.on_message(
        filters.private
        & filters.user(OWNER_ID)
        & (filters.photo | filters.document | filters.forwarded)
    )
    async def handle_forward(client: Client, message: Message):
        text = message.caption or message.text or ""

        urls = TERABOX_RE.findall(text)
        if not urls:
            await message.reply("⚠️ Koi TeraBox link nahi mila.")
            return
        terabox_url = urls[0]

        # Extract image
        image_bytes    = None
        image_filename = "thumbnail.jpg"

        if message.photo:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                tmp_path = tmp.name
            await client.download_media(message.photo.file_id, file_name=tmp_path)
            async with aiofiles.open(tmp_path, "rb") as f:
                image_bytes = await f.read()
            Path(tmp_path).unlink(missing_ok=True)

        elif (message.document
              and message.document.mime_type
              and message.document.mime_type.startswith("image/")):
            ext = (message.document.file_name or "img.jpg").rsplit(".", 1)[-1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
                tmp_path = tmp.name
            await client.download_media(message.document.file_id, file_name=tmp_path)
            async with aiofiles.open(tmp_path, "rb") as f:
                image_bytes = await f.read()
            image_filename = message.document.file_name or image_filename
            Path(tmp_path).unlink(missing_ok=True)

        else:
            await message.reply("⚠️ Image nahi mili. Post mein photo honi chahiye.")
            return

        job = Job(
            chat_id=message.chat.id,
            terabox_url=terabox_url,
            image_bytes=image_bytes,
            image_filename=image_filename,
        )
        await job_queue.put(job)
        pos = job_queue.qsize()
        await message.reply(
            f"✅ Queue mein add!\n"
            f"📍 Position: **#{pos}**\n"
            f"🔗 `{terabox_url}`"
        )

    @bot.on_message(filters.private & filters.user(OWNER_ID) & filters.command("status"))
    async def status_cmd(client: Client, message: Message):
        await message.reply(
            f"📊 **Queue Status**\n"
            f"Waiting: `{job_queue.qsize()}` jobs\n"
            f"Processing: `{len(pending_jobs)}` jobs"
        )

    @bot.on_message(filters.private & filters.user(OWNER_ID) & filters.command("start"))
    async def start_cmd(client: Client, message: Message):
        await message.reply(
            "👋 **TeraBox → Catbox Bot**\n\n"
            "Forward karo post jisme:\n"
            "• 🖼 Image ho\n"
            "• 🔗 TeraBox link ho (caption mein)\n\n"
            "Bot karega:\n"
            "1. Image → Catbox\n"
            "2. Video download → Catbox\n"
            "3. Dono links DM\n\n"
            "/status — queue dekho"
        )

    return bot

# ─── Userbot ──────────────────────────────────────────────────────────────────
def make_userbot() -> Client:
    return Client(
        "terabot_user",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=USER_SESSION,
    )

# ─── Entry Point ─────────────────────────────────────────────────────────────
async def main():
    userbot = make_userbot()
    attach_reply_monitor(userbot)
    bot = make_bot(userbot)

    await userbot.start()
    await bot.start()
    log.info("✅ Both clients started!")

    asyncio.create_task(queue_worker(bot, userbot))

    await idle()

    await bot.stop()
    await userbot.stop()

if __name__ == "__main__":
    asyncio.run(main())
