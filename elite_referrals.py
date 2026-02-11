import sqlite3
import logging
import asyncio
import json
import os
import re
import html
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

# ================= CONFIG =================
load_dotenv()

TOKEN = os.getenv("TOKEN")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID"))
except (TypeError, ValueError):
    raise ValueError("❌ ADMIN_ID يجب أن يكون رقماً صحيحاً في ملف .env")

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

if not all([TOKEN, ADMIN_ID, CHANNEL_USERNAME]):
    raise ValueError("❌ المتغيرات البيئية الناقصة: TOKEN, ADMIN_ID, CHANNEL_USERNAME")

# تأكد من أن يوزر القناة يبدأ بـ @
if not CHANNEL_USERNAME.startswith("@"):
    CHANNEL_USERNAME = f"@{CHANNEL_USERNAME}"

DEFAULT_POINTS = 100
DEFAULT_DELAY = 10
BROADCAST_LIMIT = 20  # تقليل الحد لتجنب التقييد
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================= DATABASE =================
conn = sqlite3.connect("elite_referrals.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    points INTEGER DEFAULT 0,
    last_seen TEXT,
    can_receive_broadcast INTEGER DEFAULT 1
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS referrals (
    new_user INTEGER PRIMARY KEY,
    referrer INTEGER,
    joined_at TEXT,
    counted INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS contest (
    id INTEGER PRIMARY KEY,
    active INTEGER DEFAULT 0,
    end_time TEXT,
    winners INTEGER DEFAULT 3
)
""")

cursor.execute("CREATE INDEX IF NOT EXISTS idx_referrals_counted ON referrals(counted, joined_at)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_points ON users(points DESC)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_broadcast ON users(can_receive_broadcast)")

cursor.execute("INSERT OR IGNORE INTO settings VALUES ('points', ?)", (DEFAULT_POINTS,))
cursor.execute("INSERT OR IGNORE INTO settings VALUES ('delay', ?)", (DEFAULT_DELAY,))
conn.commit()

# ================= SETTINGS =================
def get_setting(key):
    cursor.execute("SELECT value FROM settings WHERE key=?", (key,))
    result = cursor.fetchone()
    return int(result[0]) if result else (DEFAULT_POINTS if key == "points" else DEFAULT_DELAY)

def set_setting(key, value):
    cursor.execute("UPDATE settings SET value=? WHERE key=?", (value, key))
    conn.commit()

# ================= SECURITY =================
async def is_valid_member(bot, user_id):
    try:
        if not user_id or user_id < 0:
            return False
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.warning(f"فشل التحقق من العضوية للمستخدم {user_id}: {e}")
        return False

def sanitize_username(username):
    if not username:
        return None
    return re.sub(r'[^\w]', '', username)[:32] or None

def escape_html(text):
    return html.escape(str(text)) if text else ""

def is_admin(user_id):
    """التحقق من صلاحيات المشرف"""
    return user_id == ADMIN_ID

# ================= KEYBOARDS =================
def main_menu_keyboard(is_admin=False):
    """لوحة التحكم الرئيسية - تختلف حسب الصلاحيات"""
    keyboard = [
        [KeyboardButton("👤 ملفي"), KeyboardButton("🔗 رابط الإحالة")],
        [KeyboardButton("🏆 الترتيب"), KeyboardButton("🎯 حالة المسابقة")],
    ]
    if is_admin:
        keyboard.append([KeyboardButton("👑 لوحة التحكم")])
    keyboard.append([KeyboardButton("ℹ️ كيفية الاستخدام")])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def admin_panel_keyboard():
    """لوحة تحكم المشرف"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 بدء مسابقة", callback_data="start_new_contest"),
         InlineKeyboardButton("🛑 إنهاء المسابقة", callback_data="manual_end_contest")],
        [InlineKeyboardButton("📊 عرض الترتيب الكامل", callback_data="show_full_ranking")],
        [InlineKeyboardButton("⚙️ إعدادات النقاط", callback_data="settings_points"),
         InlineKeyboardButton("⏱️ إعدادات التأخير", callback_data="settings_delay")],
        [InlineKeyboardButton("📤 بث رسالة", callback_data="broadcast_menu"),
         InlineKeyboardButton("✉️ رسالة فردية", callback_data="send_menu")],
        [InlineKeyboardButton("💾 نسخ احتياطي", callback_data="backup_menu"),
         InlineKeyboardButton("🔄 استيراد بيانات", callback_data="import_menu")],
        [InlineKeyboardButton("⬅️ العودة للقائمة", callback_data="main_menu")]
    ])

def referral_keyboard(referral_link):
    """أزرار مشاركة رابط الإحالة"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📤 مشاركة الرابط", url=f"https://t.me/share/url?url={referral_link}&text=انضم%20إلى%20مسابقتي%20واربح%20الجوائز!%20✨"),
            InlineKeyboardButton("📋 نسخ الرابط", callback_data="copy_link_info")
        ],
        [
            InlineKeyboardButton("⬅️ العودة للقائمة", callback_data="main_menu")
        ]
    ])

def contest_status_keyboard(active):
    """أزرار حالة المسابقة"""
    if active:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 إنهاء المسابقة الآن", callback_data="confirm_end_contest_warning")],
            [InlineKeyboardButton("⬅️ العودة", callback_data="main_menu")]
        ])
    else:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 بدء مسابقة جديدة", callback_data="start_new_contest")],
            [InlineKeyboardButton("⬅️ العودة", callback_data="main_menu")]
        ])

def start_contest_keyboard():
    """خيارات سريعة لبدء مسابقة"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱️ 30 دقيقة - 3 فائزين", callback_data="quick_contest_30_3")],
        [InlineKeyboardButton("⏱️ 60 دقيقة - 5 فائزين", callback_data="quick_contest_60_5")],
        [InlineKeyboardButton("⏱️ 120 دقيقة - 10 فائزين", callback_data="quick_contest_120_10")],
        [InlineKeyboardButton("⚙️ تخصيص", callback_data="custom_contest_info")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_contest")]
    ])

# ================= CONTEST ENGINE =================
async def end_contest(app, force_manual=False):
    """إنهاء المسابقة وإعلان الفائزين"""
    try:
        cursor.execute("SELECT winners, active FROM contest WHERE id=1")
        result = cursor.fetchone()
        if not result or result[1] == 0:
            return False, "لا توجد مسابقة نشطة حالياً"

        winners_count = result[0]
        cursor.execute("""
            SELECT user_id, username, first_name, points 
            FROM users 
            WHERE points > 0 
            ORDER BY points DESC 
            LIMIT ?
        """, (winners_count,))
        winners = cursor.fetchall()

        if not winners:
            cursor.execute("UPDATE contest SET active=0 WHERE id=1")
            conn.commit()
            return False, "❌ لا توجد إحالات صالحة لإنهاء المسابقة"

        # إعداد قائمة الفائزين
        winner_list = []
        for i, (user_id, username, first_name, points) in enumerate(winners, 1):
            display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"ID:{user_id}")
            winner_list.append({
                "rank": i,
                "user_id": user_id,
                "display_name": display_name,
                "points": points
            })

        # رسالة للإداري
        admin_msg = "🏆 <b>انتهت المسابقة! الفائزون:</b>\n\n"
        for w in winner_list:
            admin_msg += f"🏅 المركز {w['rank']}: {w['display_name']} | {w['points']} نقطة\n"

        # رسالة للقناة
        channel_msg = "🎉 <b>مسابقة الإحالات انتهت!</b> 🎉\n\n🎊 <b>الفائزون هم:</b>\n\n"
        for w in winner_list:
            medal = "🥇" if w['rank'] == 1 else "🥈" if w['rank'] == 2 else "🥉" if w['rank'] == 3 else "🏅"
            channel_msg += f"{medal} المركز {w['rank']}: {w['display_name']}\n"
        channel_msg += "\n🎁 <i>سيتم التواصل مع الفائزين لتسليم الجوائز قريباً!</i>"

        # 1. إرسال للإداري
        try:
            await app.bot.send_message(ADMIN_ID, admin_msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"فشل إرسال رسالة للإداري: {e}")

        # 2. إرسال للفائزين
        for w in winner_list:
            try:
                rank_emoji = "🥇" if w['rank'] == 1 else "🥈" if w['rank'] == 2 else "🥉" if w['rank'] == 3 else f"🏅 #{w['rank']}"
                await app.bot.send_message(
                    w["user_id"],
                    f"🏆 <b>مبروك!</b>\n\nفزت بالمركز <b>{rank_emoji} {w['rank']}</b> في مسابقة الإحالات!\n"
                    f"💎 نقاطك: <b>{w['points']}</b>\n\n"
                    f"🎁 يرجى متابعة القناة لاستلام جائزتك قريباً!",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("عرض القناة 📢", url=f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}")]
                    ])
                )
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.warning(f"فشل إرسال إشعار للفائز {w['user_id']}: {e}")

        # 3. إرسال للقناة
        try:
            await app.bot.send_message(
                CHANNEL_USERNAME,
                channel_msg,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✨ انضم للمسابقة القادمة", url=f"https://t.me/{app.bot.username}")]
                ])
            )
        except Exception as e:
            logger.warning(f"فشل إرسال إعلان القناة: {e}")
            # محاولة إرسال بدون تنسيق HTML
            try:
                clean_msg = re.sub(r'<[^>]+>', '', channel_msg)
                await app.bot.send_message(CHANNEL_USERNAME, clean_msg)
            except Exception as e2:
                logger.error(f"فشل إرسال الإعلان حتى بدون HTML: {e2}")

        # تحديث حالة المسابقة
        cursor.execute("UPDATE contest SET active=0 WHERE id=1")
        conn.commit()
        logger.info(f"{'تم إنهاء المسابقة يدويًا' if force_manual else 'انتهت المسابقة تلقائيًا'} - الفائزون: {len(winners)}")

        return True, winner_list

    except Exception as e:
        logger.error(f"فشل إنهاء المسابقة: {e}")
        try:
            await app.bot.send_message(ADMIN_ID, f"❌ خطأ في إنهاء المسابقة: {e}")
        except:
            pass
        return False, str(e)

# ================= CONTEST COMMANDS =================
async def start_contest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر بدء مسابقة - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "🎯 <b>بدء مسابقة جديدة</b>\n\nاختر الإعدادات السريعة:",
            reply_markup=start_contest_keyboard(),
            parse_mode="HTML"
        )
        return

    try:
        minutes = int(context.args[0])
        winners = int(context.args[1])
        if minutes <= 0 or winners <= 0:
            raise ValueError("القيم يجب أن تكون موجبة")
    except Exception as e:
        await update.message.reply_text(f"⚠️ خطأ: {e}\n\nاستخدم:\n/startcontest <الدقائق> <عدد_الفائزين>")
        return

    await _create_contest(update, context, minutes, winners)

async def _create_contest(update: Update, context: ContextTypes.DEFAULT_TYPE, minutes: int, winners: int):
    """إنشاء مسابقة جديدة"""
    end_time = datetime.now(timezone.utc) + timedelta(minutes=minutes)

    cursor.execute("DELETE FROM contest")
    cursor.execute("INSERT INTO contest (id, active, end_time, winners) VALUES (1, 1, ?, ?)", 
                  (end_time.isoformat(), winners))
    cursor.execute("UPDATE users SET points=0")
    conn.commit()

    contest_msg = (
        f"🚀 <b>بدأت مسابقة الإحالات!</b>\n\n"
        f"⏰ <b>المدة:</b> {minutes} دقيقة\n"
        f"🏆 <b>عدد الفائزين:</b> {winners}\n"
        f"💎 <b>النقاط لكل إحالة:</b> {get_setting('points')}\n\n"
        f"🎯 <i>كل إحالة ناجحة تضيف نقاطاً لحسابك بعد {get_setting('delay')} دقائق</i>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 إنهاء المسابقة يدويًا", callback_data="confirm_end_contest_warning")],
        [InlineKeyboardButton("📊 عرض الترتيب الحالي", callback_data="show_contest_ranking")]
    ])
    
    await update.effective_message.reply_text(
        contest_msg,
        parse_mode="HTML",
        reply_markup=keyboard
    )
    logger.info(f"بدأت مسابقة جديدة: {minutes} دقيقة، {winners} فائزين")

async def end_contest_manual_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر إنهاء المسابقة يدويًا - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    cursor.execute("SELECT active, winners FROM contest WHERE id=1")
    contest = cursor.fetchone()
    if not contest or contest[0] == 0:
        await update.message.reply_text(
            "❌ لا توجد مسابقة نشطة حالياً",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 بدء مسابقة جديدة", callback_data="start_new_contest")]
            ])
        )
        return

    winners_count = contest[1]
    cursor.execute("""
        SELECT username, first_name, points 
        FROM users 
        WHERE points > 0 
        ORDER BY points DESC 
        LIMIT 10
    """)
    top_users = cursor.fetchall()
    
    preview = "📊 <b>الترتيب الحالي (أعلى 10):</b>\n\n"
    for i, (username, first_name, points) in enumerate(top_users, 1):
        display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"مستخدم #{i}")
        preview += f"{i}. {display_name} | {points} نقطة\n"
    if not top_users:
        preview = "📭 لا توجد إحالات مسجلة بعد"

    await update.message.reply_text(
        f"🛑 <b>هل أنت متأكد من إنهاء المسابقة يدويًا؟</b>\n\n"
        f"🏆 سيتم اختيار <b>{winners_count}</b> فائزين من الترتيب الحالي:\n\n"
        f"{preview}\n\n"
        f"⚠️ <i>لا يمكن التراجع بعد التأكيد!</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، أنهِ المسابقة الآن", callback_data="confirm_end_contest")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_end_contest")]
        ]),
        parse_mode="HTML"
    )

# ================= REFERRAL ENGINE =================
async def background_tasks(app):
    """المهمة الخلفية لمعالجة الإحالات وإنهاء المسابقات تلقائيًا"""
    while True:
        await asyncio.sleep(30)
        try:
            delay = get_setting("delay")
            points = get_setting("points")
            now = datetime.now(timezone.utc)

            # معالجة الإحالات المؤجلة
            cursor.execute("SELECT new_user, referrer, joined_at FROM referrals WHERE counted=0")
            rows = cursor.fetchall()

            for new_user, referrer, joined_at in rows:
                try:
                    joined_time = datetime.fromisoformat(joined_at.replace("Z", "+00:00"))
                    if (now - joined_time) < timedelta(minutes=delay):
                        continue

                    if not await is_valid_member(app.bot, new_user):
                        logger.info(f"المستخدم {new_user} غادر القناة - لن تحتسب إحالته")
                        continue

                    cursor.execute("SELECT user_id FROM users WHERE user_id=?", (referrer,))
                    if not cursor.fetchone():
                        logger.warning(f"محيل غير موجود: {referrer}")
                        continue

                    cursor.execute("UPDATE users SET points = points + ? WHERE user_id=?", (points, referrer))
                    cursor.execute("UPDATE referrals SET counted=1 WHERE new_user=?", (new_user,))
                    conn.commit()

                    try:
                        await app.bot.send_message(
                            referrer,
                            f"🎉 <b>تم احتساب إحالة جديدة!</b>\n+{points} نقطة 💎",
                            parse_mode="HTML"
                        )
                        logger.info(f"تم احتساب إحالة: {new_user} ← {referrer}")
                    except Exception as e:
                        logger.warning(f"فشل إرسال إشعار للمحيل {referrer}: {e}")

                except Exception as e:
                    logger.error(f"خطأ في معالجة إحالة {new_user}: {e}")
                    continue

            # إنهاء المسابقة تلقائيًا عند الوصول للوقت
            cursor.execute("SELECT active, end_time, winners FROM contest WHERE id=1")
            contest_data = cursor.fetchone()
            if contest_data and contest_data[0] == 1:
                end_time = datetime.fromisoformat(contest_data[1].replace("Z", "+00:00"))
                if now >= end_time:
                    logger.info("تم الوصول لوقت انتهاء المسابقة - بدء عملية الإنهاء التلقائي")
                    success, result = await end_contest(app, force_manual=False)
                    if success:
                        logger.info("تم إنهاء المسابقة تلقائيًا بنجاح")
                    else:
                        logger.error(f"فشل إنهاء المسابقة التلقائي: {result}")

        except Exception as e:
            logger.error(f"خطأ في المهمة الخلفية: {e}")

# ================= START & PROFILE =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start"""
    user = update.effective_user
    if not user:
        return

    safe_username = sanitize_username(user.username)
    safe_first_name = escape_html(user.first_name)[:50] if user.first_name else "مستخدم"
    now = datetime.now(timezone.utc).isoformat()
    
    # تحديث/إضافة المستخدم
    cursor.execute("""
        INSERT INTO users (user_id, username, first_name, last_seen) 
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            username=excluded.username, 
            first_name=excluded.first_name,
            last_seen=excluded.last_seen
    """, (user.id, safe_username, safe_first_name, now))
    conn.commit()

    # معالجة رابط الإحالة
    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0])
            if referrer_id == user.id:
                referrer_id = None
        except:
            pass

    if referrer_id and referrer_id != user.id:
        cursor.execute("SELECT user_id FROM users WHERE user_id=?", (referrer_id,))
        if cursor.fetchone() and await is_valid_member(context.bot, user.id):
            cursor.execute("""
                INSERT OR IGNORE INTO referrals (new_user, referrer, joined_at)
                VALUES (?, ?, ?)
            """, (user.id, referrer_id, now))
            conn.commit()
            logger.info(f"تسجيل إحالة جديدة: {user.id} ← {referrer_id}")

    # ✅ الإصلاح الأهم: رابط صحيح بدون مسافات زائدة
    bot_username = context.bot.username
    referral_link = f"https://t.me/{bot_username}?start={user.id}"

    cursor.execute("SELECT points FROM users WHERE user_id=?", (user.id,))
    points = cursor.fetchone()[0] or 0

    display_name = f"@{safe_username}" if safe_username else safe_first_name

    welcome_msg = (
        f"👋 <b>أهلاً بك يا {display_name}!</b>\n\n"
        f"💎 <b>نقاطك الحالية:</b> {points}\n"
        f"⏳ يتم احتساب النقاط بعد <b>{get_setting('delay')}</b> دقيقة من الاشتراك الفعّال.\n\n"
        f"🎯 شارك رابط الإحالة الخاص بك واجمع النقاط!"
    )
    
    await update.message.reply_text(
        welcome_msg,
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(is_admin=is_admin(user.id))
    )

async def me(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض ملف المستخدم الشخصي"""
    user = update.effective_user
    cursor.execute("SELECT points, username, first_name FROM users WHERE user_id=?", (user.id,))
    result = cursor.fetchone()
    
    if not result:
        await update.message.reply_text(
            "❌ لم يتم تسجيل حسابك بعد. أرسل /start أولًا.",
            reply_markup=main_menu_keyboard(is_admin=is_admin(user.id))
        )
        return

    points, username, first_name = result
    safe_username = sanitize_username(username)
    display_name = f"@{safe_username}" if safe_username else escape_html(first_name or "مستخدم")
    
    # ✅ رابط صحيح بدون مسافات
    bot_username = context.bot.username
    referral_link = f"https://t.me/{bot_username}?start={user.id}"

    # حالة المسابقة
    cursor.execute("SELECT active, end_time FROM contest WHERE id=1")
    contest = cursor.fetchone()
    contest_info = ""
    if contest and contest[0] == 1:
        end_time = datetime.fromisoformat(contest[1].replace("Z", "+00:00"))
        remaining = max(0, int((end_time - datetime.now(timezone.utc)).total_seconds() / 60))
        contest_info = f"🎯 <b>مسابقة نشطة!</b> ⏳ متبقي: <b>{remaining}</b> دقيقة"
    else:
        contest_info = "📭 لا توجد مسابقة نشطة حالياً"

    profile_msg = (
        f"👤 <b>ملفك الشخصي</b>\n\n"
        f"🆔 <b>معرفك:</b> <code>{user.id}</code>\n"
        f"🏷 <b>اسمك:</b> {display_name}\n"
        f"💎 <b>نقاطك:</b> {points}\n"
        f"📊 {contest_info}"
    )
    
    await update.message.reply_text(
        profile_msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 رابط الإحالة", callback_data=f"show_link_{user.id}")],
            [InlineKeyboardButton("📤 مشاركة الرابط", url=f"https://t.me/share/url?url={referral_link}&text=انضم%20إلى%20مسابقتي%20واربح%20الجوائز!%20✨")],
            [InlineKeyboardButton("🏆 عرض الترتيب", callback_data="show_ranking")],
            [InlineKeyboardButton("🎯 حالة المسابقة", callback_data="show_contest_status")]
        ])
    )

# ================= ADMIN PANEL & COMMANDS =================
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض لوحة تحكم المشرف"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    panel_msg = (
        "👑 <b>لوحة تحكم المشرف</b>\n\n"
        "اختر الإجراء المطلوب من الأزرار أدناه:"
    )
    
    await update.message.reply_text(
        panel_msg,
        parse_mode="HTML",
        reply_markup=admin_panel_keyboard()
    )

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض الترتيب العام (للجميع)"""
    cursor.execute("""
        SELECT username, first_name, points 
        FROM users 
        WHERE points > 0 
        ORDER BY points DESC 
        LIMIT 10
    """)
    rows = cursor.fetchall()

    if not rows:
        await update.message.reply_text("📭 لا توجد نقاط مسجلة بعد.")
        return

    text = "🏆 <b>الترتيب العام (أعلى 10):</b>\n\n"
    for i, (username, first_name, points) in enumerate(rows, 1):
        safe_username = sanitize_username(username)
        display_name = f"@{safe_username}" if safe_username else escape_html(first_name or f"مستخدم #{i}")
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
        text += f"{medal} {i}. {display_name} | {points} نقطة\n"

    await update.message.reply_text(text, parse_mode="HTML")

async def set_points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تعيين نقاط الإحالة - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return
        
    if not context.args:
        current = get_setting("points")
        await update.message.reply_text(
            f"⚙️ <b>إعدادات النقاط الحالية:</b> {current}\n\n"
            f"للتغيير، استخدم:\n/setpoints <القيمة>\nمثال: /setpoints 150",
            parse_mode="HTML"
        )
        return

    try:
        value = int(context.args[0])
        if value < 1:
            raise ValueError
        set_setting("points", value)
        await update.message.reply_text(f"✅ تم تعيين النقاط لكل إحالة: <b>{value}</b>", parse_mode="HTML")
        logger.info(f"تم تغيير قيمة النقاط إلى {value} بواسطة {update.effective_user.id}")
    except:
        await update.message.reply_text("❌ أدخل رقمًا صحيحًا وموجبًا")

async def set_delay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تعيين تأخير احتساب الإحالة - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return
        
    if not context.args:
        current = get_setting("delay")
        await update.message.reply_text(
            f"⚙️ <b>إعدادات التأخير الحالية:</b> {current} دقيقة\n\n"
            f"للتغيير، استخدم:\n/setdelay <القيمة>\nمثال: /setdelay 15",
            parse_mode="HTML"
        )
        return

    try:
        value = int(context.args[0])
        if value < 1 or value > 1440:
            raise ValueError
        set_setting("delay", value)
        await update.message.reply_text(f"✅ تم تعيين مدة التأخير: <b>{value}</b> دقيقة", parse_mode="HTML")
        logger.info(f"تم تغيير مدة التأخير إلى {value} دقيقة بواسطة {update.effective_user.id}")
    except:
        await update.message.reply_text("❌ أدخل رقمًا بين 1 و 1440")

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تصفير جميع النقاط - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return
        
    cursor.execute("UPDATE users SET points=0")
    conn.commit()
    await update.message.reply_text("✅ تم تصفير جميع النقاط بنجاح")
    logger.warning(f"تم تصفير النقاط بواسطة {update.effective_user.id}")

async def send_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إرسال رسالة فردية - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "✉️ <b>إرسال رسالة فردية</b>\n\n"
            "الاستخدام:\n<code>/send &lt;user_id&gt; &lt;الرسالة&gt;</code>\n\n"
            "مثال:\n<code>/send 123456789 مرحباً! تم قبول إحالتك ✅</code>",
            parse_mode="HTML"
        )
        return

    try:
        user_id = int(context.args[0])
        message_text = " ".join(context.args[1:])
        
        if not message_text.strip():
            await update.message.reply_text("❌ الرسالة فارغة!")
            return

        safe_message = escape_html(message_text.strip())

        cursor.execute("SELECT user_id, username, first_name FROM users WHERE user_id=?", (user_id,))
        user = cursor.fetchone()
        if not user:
            await update.message.reply_text(f"❌ المستخدم {user_id} غير مسجل في النظام")
            return

        # إرسال الرسالة
        await context.bot.send_message(
            chat_id=user_id,
            text=f"📩 <b>رسالة من الإدارة:</b>\n\n{safe_message}",
            parse_mode="HTML"
        )
        
        # إشعار للإداري
        username, first_name = user[1], user[2]
        display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"ID:{user_id}")
        await update.message.reply_text(
            f"✅ تم إرسال الرسالة إلى:\n{display_name} (ID: {user_id})"
        )
        logger.info(f"رسالة فردية أرسلت إلى {user_id} بواسطة {ADMIN_ID}")

    except ValueError:
        await update.message.reply_text("❌ معرف المستخدم يجب أن يكون رقمًا")
    except Exception as e:
        error_msg = str(e)
        if "bot was blocked" in error_msg.lower():
            await update.message.reply_text(f"❌ فشل الإرسال: المستخدم حظر البوت")
            cursor.execute("UPDATE users SET can_receive_broadcast=0 WHERE user_id=?", (user_id,))
            conn.commit()
        else:
            await update.message.reply_text(f"❌ خطأ في الإرسال: {error_msg}")
        logger.error(f"فشل إرسال رسالة فردية إلى {user_id}: {e}")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إرسال بث جماعي - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    if not context.args:
        await update.message.reply_text(
            "📢 <b>بث رسالة جماعية</b>\n\n"
            "الاستخدام:\n<code>/broadcast &lt;الرسالة&gt;</code>\n\n"
            "مثال:\n<code>/broadcast مسابقة جديدة تبدأ بعد ساعة! 🚀</code>",
            parse_mode="HTML"
        )
        return

    message_text = " ".join(context.args).strip()
    if not message_text:
        await update.message.reply_text("❌ الرسالة فارغة!")
        return

    if len(message_text) > 4000:
        await update.message.reply_text("❌ الرسالة طويلة جدًا (الحد الأقصى 4000 حرف)")
        return

    preview = message_text[:100] + "..." if len(message_text) > 100 else message_text
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ تأكيد الإرسال", callback_data=f"confirm_broadcast|{message_text}")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_broadcast")]
    ])
    
    await update.message.reply_text(
        f"📢 <b>معاينة البث:</b>\n\n{escape_html(preview)}\n\n"
        f"هل تريد إرسال هذه الرسالة لجميع المستخدمين؟",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

async def export_data_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تصدير نسخة احتياطية - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    try:
        data = {
            "metadata": {
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "version": "2.1",
                "channel": CHANNEL_USERNAME
            },
            "users": [],
            "referrals": [],
            "settings": [],
            "contest": []
        }

        for table in ["users", "referrals", "settings", "contest"]:
            cursor.execute(f"SELECT * FROM {table}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            data[table] = [dict(zip(columns, row)) for row in rows]

        filename = f"backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        await update.message.reply_document(
            document=open(filename, "rb"),
            caption="✅ تم إنشاء نسخة احتياطية بنجاح"
        )
        os.remove(filename)
        logger.info("تم تصدير البيانات بنجاح")
    except Exception as e:
        logger.error(f"فشل التصدير: {e}")
        await update.message.reply_text(f"❌ خطأ في التصدير: {e}")

async def import_data_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استيراد نسخة احتياطية - للمشرفين فقط"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ هذا الأمر متاح للمشرفين فقط")
        return

    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("❌ الرجاء الرد على ملف JSON صالح")
        return

    if not update.message.reply_to_message.document.file_name.endswith('.json'):
        await update.message.reply_text("❌ الملف يجب أن يكون بصيغة JSON")
        return

    try:
        file = await update.message.reply_to_message.document.get_file()
        await file.download_to_drive("import_temp.json")

        with open("import_temp.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        required_keys = {"users", "referrals", "settings", "contest", "metadata"}
        if not all(key in data for key in required_keys):
            raise ValueError("هيكل الملف غير صالح - مفقود أقسام أساسية")

        version = data["metadata"].get("version", "1.0")
        if version not in ["1.1", "1.2", "1.3", "2.0", "2.1"]:
            raise ValueError(f"إصدار النسخة الاحتياطية ({version}) غير متوافق")

        for user in data["users"]:
            if not isinstance(user.get("user_id"), int) or user["user_id"] <= 0:
                raise ValueError(f"بيانات مستخدم غير صالحة: {user}")
            if user.get("points", 0) < 0:
                raise ValueError("النقاط لا يمكن أن تكون سالبة")

        # استيراد آمن باستخدام معاملة
        conn.execute("BEGIN TRANSACTION")
        try:
            for table in ["users", "referrals", "settings", "contest"]:
                cursor.execute(f"DELETE FROM {table}")

            for user in data["users"]:
                cursor.execute(
                    "INSERT INTO users (user_id, username, first_name, points, last_seen, can_receive_broadcast) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        user["user_id"],
                        sanitize_username(user.get("username")),
                        escape_html(user.get("first_name", "مستخدم"))[:50],
                        max(0, user.get("points", 0)),
                        user.get("last_seen") or datetime.now(timezone.utc).isoformat(),
                        user.get("can_receive_broadcast", 1)
                    )
                )

            for ref in data["referrals"]:
                cursor.execute(
                    "INSERT INTO referrals (new_user, referrer, joined_at, counted) VALUES (?, ?, ?, ?)",
                    (ref["new_user"], ref["referrer"], ref["joined_at"], ref["counted"])
                )

            for setting in data["settings"]:
                cursor.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?)",
                    (setting["key"], setting["value"])
                )

            for contest in data["contest"]:
                cursor.execute(
                    "INSERT INTO contest (id, active, end_time, winners) VALUES (?, ?, ?, ?)",
                    (contest["id"], contest["active"], contest["end_time"], contest["winners"])
                )

            conn.commit()
            logger.info("تم استيراد البيانات بنجاح")
            await update.message.reply_text("✅ تم الاستيراد بنجاح مع التحقق الأمني")
        except Exception as e:
            conn.rollback()
            raise e

    except Exception as e:
        logger.error(f"فشل الاستيراد الآمن: {e}")
        await update.message.reply_text(f"❌ خطأ في الاستيراد الآمن: {e}")
    finally:
        if os.path.exists("import_temp.json"):
            os.remove("import_temp.json")

# ================= MESSAGE HANDLERS =================
async def handle_menu_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أزرار القائمة السفلية (الردود النصية)"""
    if not update.message or not update.message.text:
        return
        
    text = update.message.text.strip()
    user_id = update.effective_user.id
    
    if text == "👤 ملفي":
        await me(update, context)
    elif text == "🔗 رابط الإحالة":
        bot_username = context.bot.username
        referral_link = f"https://t.me/{bot_username}?start={user_id}"  # ✅ رابط صحيح
        
        await update.message.reply_text(
            f"🔗 <b>رابط الإحالة الخاص بك:</b>\n<code>{referral_link}</code>\n\n"
            f"🎯 شاركه مع أصدقائك لجمع النقاط!",
            parse_mode="HTML",
            reply_markup=referral_keyboard(referral_link)
        )
    elif text == "🏆 الترتيب":
        await top_command(update, context)
    elif text == "🎯 حالة المسابقة":
        cursor.execute("SELECT active, end_time, winners FROM contest WHERE id=1")
        contest = cursor.fetchone()
        
        if not contest or contest[0] == 0:
            msg = "📭 <b>لا توجد مسابقة نشطة حالياً</b>\n\n🚀 ابدأ مسابقة جديدة لجمع النقاط!"
            keyboard = contest_status_keyboard(False)
        else:
            end_time = datetime.fromisoformat(contest[1].replace("Z", "+00:00"))
            remaining = max(0, int((end_time - datetime.now(timezone.utc)).total_seconds() / 60))
            msg = (
                f"🎯 <b>مسابقة نشطة!</b>\n\n"
                f"⏰ الوقت المتبقي: <b>{remaining}</b> دقيقة\n"
                f"🏆 عدد الفائزين: <b>{contest[2]}</b>\n"
                f"💎 النقاط لكل إحالة: <b>{get_setting('points')}</b>"
            )
            keyboard = contest_status_keyboard(True)
        
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
    elif text == "ℹ️ كيفية الاستخدام":
        await update.message.reply_text(
            "🎯 <b>كيفية استخدام البوت:</b>\n\n"
            f"1️⃣ اشترك أولاً في القناة: {CHANNEL_USERNAME}\n"
            "2️⃣ اضغط على <b>رابط الإحالة</b> وشاركه مع أصدقائك\n"
            f"3️⃣ كل صديق يشترك عبر رابطك يضيف <b>{get_setting('points')}</b> نقطة لحسابك بعد <b>{get_setting('delay')}</b> دقيقة\n"
            "4️⃣ تصدر الترتيب واربح الجوائز في المسابقات! 🏆",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("عرض القناة 📢", url=f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}")]
            ])
        )
    elif text == "👑 لوحة التحكم":
        if is_admin(user_id):
            await admin_panel(update, context)
        else:
            await update.message.reply_text("❌ هذا القسم متاح للمشرفين فقط")

async def unified_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالج موحد لجميع أزرار التفاعل (Inline Buttons)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # ✅ الإجابة الفورية لتجنب مؤشر التحميل الأبدي
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"فشل الإجابة على الكول باك: {e}")
    
    try:
        data = query.data
        
        # معالجة نسخ الرابط (إشعار توضيحي)
        if data == "copy_link_info":
            await query.answer(
                "✅ للنسخ: اضغط مطولًا على الرابط أعلاه واختر 'نسخ الرابط'",
                show_alert=True
            )
            return
        
        # العودة للقائمة الرئيسية
        if data == "main_menu":
            await query.message.reply_text(
                "🏠 <b>القائمة الرئيسية</b>",
                reply_markup=main_menu_keyboard(is_admin=is_admin(user_id)),
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # عرض رابط الإحالة
        if data.startswith("show_link_"):
            target_user_id = int(data.split("_")[2])
            cursor.execute("SELECT points FROM users WHERE user_id=?", (target_user_id,))
            points = cursor.fetchone()[0] or 0
            
            bot_username = context.bot.username
            referral_link = f"https://t.me/{bot_username}?start={target_user_id}"  # ✅ رابط صحيح
            
            await query.message.reply_text(
                f"🔗 <b>رابط الإحالة:</b>\n<code>{referral_link}</code>\n\n💎 <b>نقاطك:</b> {points}",
                parse_mode="HTML",
                reply_markup=referral_keyboard(referral_link)
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # عرض الترتيب
        if data == "show_ranking":
            cursor.execute("""
                SELECT username, first_name, points 
                FROM users 
                WHERE points > 0 
                ORDER BY points DESC 
                LIMIT 10
            """)
            rows = cursor.fetchall()
            
            text = "🏆 <b>العشرة الأوائل:</b>\n\n" if rows else "📭 لا توجد نقاط بعد"
            for i, (username, first_name, points) in enumerate(rows, 1):
                display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"مستخدم {i}")
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
                text += f"{medal} {i}. {display_name} | {points} نقطة\n"
            
            await query.message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ رجوع", callback_data="main_menu")]
                ])
            )
            return
        
        # حالة المسابقة
        if data == "show_contest_status":
            cursor.execute("SELECT active, end_time, winners FROM contest WHERE id=1")
            contest = cursor.fetchone()
            
            if not contest or contest[0] == 0:
                msg = "📭 <b>لا توجد مسابقة نشطة</b>"
                keyboard = contest_status_keyboard(False)
            else:
                end_time = datetime.fromisoformat(contest[1].replace("Z", "+00:00"))
                remaining = max(0, int((end_time - datetime.now(timezone.utc)).total_seconds() / 60))
                msg = f"🎯 <b>مسابقة نشطة!</b>\n⏰ متبقي: <b>{remaining}</b> دقيقة\n🏆 فائزون: <b>{contest[2]}</b>"
                keyboard = contest_status_keyboard(True)
            
            await query.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
            return
        
        # بدء مسابقة جديدة (من لوحة التحكم)
        if data == "start_new_contest":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
            await query.message.reply_text(
                "🎯 <b>بدء مسابقة جديدة</b>\n\nاختر الإعدادات السريعة:",
                reply_markup=start_contest_keyboard(),
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # إنهاء المسابقة (تحذير أولي)
        if data == "confirm_end_contest_warning":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            cursor.execute("SELECT active, winners FROM contest WHERE id=1")
            contest = cursor.fetchone()
            if not contest or contest[0] == 0:
                await query.answer("لا توجد مسابقة نشطة", show_alert=True)
                return
                
            await query.message.reply_text(
                "🛑 <b>تأكيد إنهاء المسابقة</b>\n\n"
                "هل أنت متأكد من إنهاء المسابقة يدويًا؟\n"
                "سيتم اختيار الفائزين فورًا وإعلانهم.\n\n"
                "⚠️ لا يمكن التراجع بعد التأكيد!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ نعم، أنهِ الآن", callback_data="confirm_end_contest")],
                    [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_end_contest")]
                ]),
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # تأكيد إنهاء المسابقة
        if data == "confirm_end_contest":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            await query.edit_message_text("🔄 جاري إنهاء المسابقة وإعلان الفائزين...")
            success, result = await end_contest(context.application, force_manual=True)
            if success:
                summary = "✅ <b>تم إنهاء المسابقة بنجاح!</b>\n\n🏆 الفائزون:\n"
                for w in result:
                    medal = "🥇" if w['rank'] == 1 else "🥈" if w['rank'] == 2 else "🥉" if w['rank'] == 3 else "🏅"
                    summary += f"{medal} {w['rank']}. {w['display_name']} ({w['points']} نقطة)\n"
                await query.edit_message_text(summary, parse_mode="HTML")
            else:
                await query.edit_message_text(f"❌ فشل إنهاء المسابقة:\n{result}")
            return
        
        # إلغاء العمليات
        if data in ["cancel_end_contest", "cancel_contest"]:
            await query.edit_message_text("❌ تم إلغاء العملية")
            return
        
        # بدء مسابقة سريعة
        if data.startswith("quick_contest_"):
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            parts = data.replace("quick_contest_", "").split("_")
            minutes, winners = int(parts[0]), int(parts[1])
            await _create_contest(query, context, minutes, winners)
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # عرض الترتيب الكامل (للمشرفين)
        if data == "show_full_ranking":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            cursor.execute("""
                SELECT username, first_name, points 
                FROM users 
                WHERE points > 0 
                ORDER BY points DESC 
                LIMIT 50
            """)
            rows = cursor.fetchall()
            
            if not rows:
                text = "📭 لا توجد نقاط مسجلة بعد"
            else:
                text = "🏆 <b>الترتيب الكامل (أعلى 50):</b>\n\n"
                for i, (username, first_name, points) in enumerate(rows, 1):
                    display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"مستخدم {i}")
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
                    text += f"{medal} {i}. {display_name} | {points}\n"
            
            await query.message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ رجوع", callback_data="main_menu")]
                ])
            )
            return
        
        # عرض الترتيب الحالي للمسابقة
        if data == "show_contest_ranking":
            cursor.execute("""
                SELECT username, first_name, points 
                FROM users 
                WHERE points > 0 
                ORDER BY points DESC 
                LIMIT 10
            """)
            rows = cursor.fetchall()
            
            text = "📊 <b>الترتيب الحالي للمسابقة:</b>\n\n" if rows else "📭 لا توجد نقاط بعد"
            for i, (username, first_name, points) in enumerate(rows, 1):
                display_name = f"@{sanitize_username(username)}" if username else escape_html(first_name or f"مستخدم {i}")
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
                text += f"{medal} {i}. {display_name} | {points}\n"
            
            await query.message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ رجوع", callback_data="main_menu")]
                ])
            )
            return
        
        # إعدادات النقاط
        if data == "settings_points":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            current = get_setting("points")
            await query.message.reply_text(
                f"⚙️ <b>إعدادات النقاط الحالية:</b> {current}\n\n"
                f"للتغيير، استخدم الأمر:\n<code>/setpoints &lt;القيمة&gt;</code>",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # إعدادات التأخير
        if data == "settings_delay":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            current = get_setting("delay")
            await query.message.reply_text(
                f"⏱️ <b>إعدادات التأخير الحالية:</b> {current} دقيقة\n\n"
                f"للتغيير، استخدم الأمر:\n<code>/setdelay &lt;القيمة&gt;</code>",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # قائمة البث
        if data == "broadcast_menu":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            await query.message.reply_text(
                "📢 <b>بث رسالة جماعية</b>\n\n"
                f"لإرسال بث، استخدم الأمر:\n<code>/broadcast &lt;الرسالة&gt;</code>\n\n"
                "مثال:\n<code>/broadcast مسابقة جديدة تبدأ بعد ساعة! 🚀</code>",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # قائمة الرسائل الفردية
        if data == "send_menu":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            await query.message.reply_text(
                "✉️ <b>إرسال رسالة فردية</b>\n\n"
                f"لإرسال رسالة، استخدم الأمر:\n<code>/send &lt;user_id&gt; &lt;الرسالة&gt;</code>\n\n"
                "مثال:\n<code>/send 123456789 مرحباً! تم قبول إحالتك ✅</code>",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # قائمة النسخ الاحتياطي
        if data == "backup_menu":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            await query.message.reply_text(
                "💾 <b>النسخ الاحتياطي</b>\n\n"
                f"لإنشاء نسخة احتياطية، استخدم الأمر:\n<code>/export</code>\n\n"
                "سيتم إرسال ملف JSON يحتوي على جميع البيانات.",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return
        
        # قائمة الاستيراد
        if data == "import_menu":
            if not is_admin(user_id):
                await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
                return
                
            await query.message.reply_text(
                "🔄 <b>استيراد البيانات</b>\n\n"
                "1. قم بتصدير ملف احتياطي سابق (باستخدام /export)\n"
                "2. أرسل الملف هنا في المحادثة\n"
                "3. رد على الملف بأمر:\n<code>/import</code>",
                parse_mode="HTML"
            )
            try:
                await query.message.delete()
            except:
                pass
            return

    except Exception as e:
        logger.error(f"خطأ في معالج الكول باك: {e}")
        try:
            await query.answer(f"❌ حدث خطأ: {str(e)[:50]}", show_alert=True)
        except:
            pass

async def broadcast_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة تأكيد البث الجماعي"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "cancel_broadcast":
        await query.edit_message_text("❌ تم إلغاء عملية البث")
        return

    if query.data.startswith("confirm_broadcast|"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ هذا الإجراء متاح للمشرفين فقط", show_alert=True)
            return
            
        message_text = query.data.split("|", 1)[1]
        safe_message = escape_html(message_text.strip())

        cursor.execute("SELECT user_id FROM users WHERE can_receive_broadcast=1")
        users = cursor.fetchall()
        total = len(users)
        
        status_msg = await query.edit_message_text(
            f"📤 جاري إرسال البث إلى {total} مستخدم..."
        )

        success, failed = 0, 0
        for i, (user_id,) in enumerate(users, 1):
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 <b>إعلان:</b>\n\n{safe_message}",
                    parse_mode="HTML"
                )
                success += 1
                
                # تحديث التقدم كل 20 مستخدم
                if i % BROADCAST_LIMIT == 0:
                    try:
                        await status_msg.edit_text(
                            f"📤 جاري الإرسال...\n"
                            f"✅ ناجح: {success} | ❌ فشل: {failed} | 📊 {i}/{total}"
                        )
                    except:
                        pass
                    await asyncio.sleep(1)  # تأخير لتجنب التقييد
                
            except Exception as e:
                failed += 1
                error_msg = str(e).lower()
                # تعطيل المستخدم إذا حظر البوت
                if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
                    cursor.execute("UPDATE users SET can_receive_broadcast=0 WHERE user_id=?", (user_id,))
                    conn.commit()
            
            await asyncio.sleep(0.2)  # تأخير صغير بين كل رسالة

        result_msg = (
            f"✅ <b>اكتمل البث بنجاح!</b>\n\n"
            f"📊 الإحصائيات:\n"
            f"✅ ناجح: {success}\n"
            f"❌ فشل: {failed}\n"
            f"👥 المجموع: {total}"
        )
        try:
            await status_msg.edit_text(result_msg, parse_mode="HTML")
        except:
            await query.message.reply_text(result_msg, parse_mode="HTML")
        logger.info(f"اكتمل البث: ناجح {success} / فشل {failed} من أصل {total}")

# ================= SHUTDOWN HANDLER =================
async def shutdown(app):
    """إغلاق آمن لاتصال قاعدة البيانات"""
    try:
        conn.close()
        logger.info("تم إغلاق اتصال قاعدة البيانات بنجاح")
    except Exception as e:
        logger.error(f"خطأ أثناء الإغلاق: {e}")

# ================= MAIN =================
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    # تسجيل معالجات الأوامر
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(CommandHandler("top", top_command))
    app.add_handler(CommandHandler("startcontest", start_contest_command))
    app.add_handler(CommandHandler("endcontest", end_contest_manual_command))
    app.add_handler(CommandHandler("setpoints", set_points_command))
    app.add_handler(CommandHandler("setdelay", set_delay_command))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("send", send_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("export", export_data_command))
    app.add_handler(CommandHandler("import", import_data_command))
    app.add_handler(CommandHandler("panel", admin_panel))  # أمر بديل للوحة التحكم

    # تسجيل معالجات الرسائل والأزرار
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_buttons))
    app.add_handler(CallbackQueryHandler(unified_callback_handler))
    app.add_handler(CallbackQueryHandler(broadcast_callback_handler, pattern=r"^confirm_broadcast\|"))

    # تشغيل المهمة الخلفية بعد 2 ثانية لتجنب التحذير
    app.job_queue.run_once(lambda _: asyncio.create_task(background_tasks(app)), 2)

    # معالجة الإغلاق النظيف
    import signal
    def graceful_shutdown(signum, frame):
        logger.info("جارٍ الإغلاق النظيف...")
        asyncio.create_task(shutdown(app))
        exit(0)
    
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    logger.info("🚀 Elite Referral Bot يعمل الآن...")
    print("="*50)
    print("✅ البوت نشط ويعمل بشكل كامل!")
    print("="*50)
    print("✨ الميزات المضافة:")
    print("   • واجهة جميلة بأزرار تفاعلية تعمل 100%")
    print("   • لا يشترط وجود username للمستخدمين")
    print("   • مسابقات مع إنهاء يدوي/تلقائي وإعلان فائزين")
    print("   • نظام بث جماعي ورسائل فردية آمن")
    print("   • نسخ احتياطي واستيراد بيانات")
    print("   • حماية كاملة لصلاحيات المشرف")
    print("="*50)
    print(f"🤖 يوزر البوت: ")
    print(f"👑 معرف المشرف: {ADMIN_ID}")
    print(f"📢 القناة: {CHANNEL_USERNAME}")
    print("="*50)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()