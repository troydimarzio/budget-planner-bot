import re
import io
import csv
import calendar
from datetime import datetime, timedelta, date # Import 'date' object

import logging
import os
import json

from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ConversationHandler,
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
import gspread

# Cek dan buat credentials.json dari GOOGLE_CREDS_JSON environment
if os.getenv("GOOGLE_CREDS_JSON"):
    with open("credentials.json", "w") as f:
        json.dump(json.loads(os.getenv("GOOGLE_CREDS_JSON")), f)

# --- Konfigurasi Bot Telegram ---
TOKEN = "8014854736:AAHuEfN_Y4d6i_aesR-JcOAbLi70DKL3HDs"

# --- Konfigurasi Google Sheets ---
GOOGLE_SHEETS_CREDENTIALS_FILE = 'credentials.json'
# Ganti ini dengan ID Spreadsheet Google Sheet Anda yang sebenarnya
# Anda bisa menemukan ID ini di URL Google Sheet Anda (bagian antara /d/ dan /edit)
GOOGLE_SHEET_ID = '1U2hN1cckQ0zzQk7yFEDDyq6-y9SCJ8aM6Tm3MiQPA-w' # ID Spreadsheet Anda
WORKSHEET_NAME = '📋Transaksi' # Nama worksheet yang digunakan untuk transaksi

# --- Konfigurasi Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Inisialisasi Google Sheets Client ---
gc = None
worksheet = None

def init_google_sheets():
    """Menginisialisasi koneksi ke Google Sheets menggunakan kredensial yang diberikan."""
    global gc, worksheet
    try:
        if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
            logger.error(f"File kredensial Google Sheets tidak ditemukan: '{GOOGLE_SHEETS_CREDENTIALS_FILE}'. Pastikan jalur sudah benar.")
            return

        gc = gspread.service_account(filename=GOOGLE_SHEETS_CREDENTIALS_FILE)
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        
        # Inisialisasi worksheet transaksi
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME) 
        logger.info(f"Berhasil terhubung ke Google Sheet dengan ID: '{GOOGLE_SHEET_ID}', Worksheet: '{WORKSHEET_NAME}'")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet Google Sheets tidak ditemukan dengan ID: '{GOOGLE_SHEET_ID}'. Pastikan ID sudah benar dan bot memiliki akses.")
        gc = None
        worksheet = None
    except gspread.exceptions.WorksheetNotFound as e:
        logger.error(f"Worksheet Google Sheets tidak ditemukan: {e}. Pastikan nama sudah benar.")
        gc = None
        worksheet = None
    except Exception as e:
        logger.error(f"Gagal terhubung ke Google Sheets: {e}", exc_info=True)
        logger.warning(f"Pastikan:\n1. Nama file kredensial JSON di kode sudah benar ('{GOOGLE_SHEETS_CREDENTIALS_FILE}').\n2. File JSON tersebut ada di folder yang sama dengan skrip.\n3. ID Google Sheet ('{GOOGLE_SHEET_ID}') dan nama tab/worksheet ('{WORKSHEET_NAME}') di kode sudah sama persis dengan di Google Sheets.\n4. Email service account Anda (dari file JSON) sudah dibagikan ke Google Sheet dengan akses 'Editor'.")
        gc = None
        worksheet = None

# --- Daftar Kategori Utama, Sub Kategori, dan Posisi Kas ---
MAIN_CATEGORIES = ['Penghasilan', 'Pengeluaran', 'Tagihan', 'Hutang', 'Investasi']

PENGHASILAN_SUB_CATEGORIES = [
    'Gaji dan Tunjangan', 'Bisnis', 'Sampingan', 'Dividen', 'Bunga', 'Komisi', 'Lain-lain'
]

PENGELUARAN_SUB_CATEGORIES = [
    'Makanan & Minuman', 'Tempat Tinggal', 'Utilitas', 'Transportasi', 'Rumah Tangga',
    'Kesehatan', 'Pendidikan', 'Pengembangan Diri', 'Perawatan Diri', 'Pakaian & Aksesori',
    'Hiburan', 'Kehidupan Sosial', 'Hadiah', 'Liburan', 'Biaya Tak Terduga',
    'Anak & Keluarga', 'Donasi', 'Peliharaan', 'Lain-lain'
]

TAGIHAN_SUB_CATEGORIES = [
    'Tagihan Internet', 'Listrik', 'Air', 'Pulsa & Paket Data', 'Asuransi Jiwa',
    'Asuransi Kesehatan', 'Iuran Kebersihan', 'Gas Rumah Tangga', 'TV Kabel',
    'Iuran Lingkungan', 'BPJS Kesehatan', 'Langganan Aplikasi', 'Sewa Rumah',
    'Layanan Keamanan', 'Perawatan Rumah', 'Lain-lain'
]

HUTANG_SUB_CATEGORIES = [
    'Cicilan KPR', 'Cicilan Kendaraan', 'Pinjaman Pribadi', 'Tagihan Medis',
    'Kartu Kredit', 'Pinjaman Online', 'Cicilan Elektronik', 'Pinjaman Personal',
    'Kredit Tanpa Agunan', 'Lain-lain'
]

INVESTASI_SUB_CATEGORIES = [
    'Investasi Saham', 'Reksadana', 'Logam Mulia', 'Kripto', 'Deposito',
    'Properti', 'Tanah', 'Lain-lain'
]

POSISI_KAS_OPTIONS = [
    'Bank BCA ', 'Tunai', 'Bank Mandiri ', 'Bank BRI ',
    'Bank BNI ', 'SeaBank ', 'OVO', 'GoPay', 'Dana', 'Lain-lain'
]

# --- States untuk ConversationHandler ---
CHOOSING_MAIN_CATEGORY, CHOOSING_SUB_CATEGORY, ASKING_AMOUNT, ASKING_POSISI_KAS, ASKING_KETERANGAN = range(5)
DELETE_ASKING_ID, DELETE_CONFIRMATION = range(5, 7) 
EDIT_ASKING_ID, EDIT_CHOOSING_FIELD, EDIT_ASKING_NEW_VALUE, EDIT_CONFIRMATION = range(7, 11)

# Status baru untuk percakapan reset data
RESET_DATA_CONFIRMATION = range(14, 15)


# --- Fungsi Utilitas ---
def clean_numeric_string(s):
    """
    Membersihkan string yang mungkin mengandung 'Rp', '.', dan ',' menjadi angka integer.
    Mendukung input int atau float secara langsung.
    """
    if s is None or s == '':
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    
    s = str(s).strip()
    s = re.sub(r'[Rr][Pp]\s*|IDR\s*|\.|\s', '', s)
    
    if ',' in s:
        s = s.split(',')[0]
    
    try:
        return int(s)
    except ValueError:
        logger.warning(f"Gagal mengonversi '{s}' menjadi angka. Mengembalikan 0.")
        return 0

def format_rupiah(amount):
    """Memformat angka menjadi string mata uang Rupiah (contoh: Rp1.000.000)."""
    return f"Rp{amount:,.0f}".replace(",", ".")

def parse_date_string(date_str):
    """
    Mencoba memparsing string tanggal dengan berbagai format.
    Prioritas: YYYY-MM-DD, DD Bulan YYYY, DD/MM/YYYY, DD/MM/YY.
    """
    formats = ["%Y-%m-%d", "%d %B %Y", "%d/%m/%Y", "%d/%m/%y"] # Ditambahkan %d/%m/%y
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Tidak dapat memparsing tanggal: {date_str}. Format yang didukung: YYYY-MM-DD, DD Bulan YYYY, DD/MM/YYYY, DD/MM/YY.")

def get_records_with_custom_header(worksheet_obj, header_row_index=3):
    """
    Mengambil semua catatan dari worksheet, dengan asumsi header berada di baris tertentu
    dan data dimulai segera setelah baris tersebut.
    Mengembalikan daftar kamus, mirip dengan get_all_records(), tetapi menangani baris header kustom.
    
    Args:
        worksheet_obj: Objek gspread worksheet.
        header_row_index: Indeks baris berbasis 1 yang berisi header.
                          (misalnya, 3 untuk header di baris 3).
    """
    all_values = worksheet_obj.get_all_values()
    if not all_values:
        return []

    # Sesuaikan untuk daftar berbasis 0: header_row_index (berbasis 1) menjadi header_row_list_index (berbasis 0)
    header_row_list_index = header_row_index - 1

    if len(all_values) <= header_row_list_index:
        logger.error(f"Indeks baris header {header_row_index} di luar batas untuk sheet dengan {len(all_values)} baris. Tidak dapat mengambil catatan.")
        return []

    headers = all_values[header_row_list_index]
    # Bersihkan header (hapus spasi) dan tangani potensi header kosong
    cleaned_headers = [h.strip() if h else f"EMPTY_HEADER_{i}" for i, h in enumerate(headers)]
    logger.info(f"Header dibaca dari GSheet (dibersihkan): {cleaned_headers}") 
    
    records = []
    # Data dimulai dari baris segera setelah baris header
    for row_index, row_values in enumerate(all_values[header_row_list_index + 1:]):
        record = {}
        # Pastikan row_values memiliki cukup elemen untuk mencocokkan header
        for i, header in enumerate(cleaned_headers):
            if i < len(row_values):
                record[header] = row_values[i]
            else:
                record[header] = '' # Isi dengan string kosong jika nilai hilang
        records.append(record)
    return records


async def get_summary_data(user_id, start_date, end_date):
    """Mengambil dan menghitung ringkasan pemasukan/pengeluaran untuk rentang tanggal tertentu."""
    total_pemasukan = 0
    total_pengeluaran = 0

    if worksheet:
        try:
            # Gunakan fungsi header kustom
            all_records = get_records_with_custom_header(worksheet)
            
            for record in all_records:
                record_user_id = str(record.get('USER ID')) 
                record_date_str = record.get('TANGGAL') 
                
                if record_user_id == str(user_id) and record_date_str:
                    try:
                        record_date = parse_date_string(record_date_str)

                        if start_date <= record_date <= end_date:
                            uang_masuk = clean_numeric_string(record.get('UANG MASUK', 0)) 
                            uang_keluar = clean_numeric_string(record.get('UANG KELUAR', 0)) 

                            total_pemasukan += uang_masuk
                            total_pengeluaran += uang_keluar

                    except ValueError:
                        logger.warning(f"PERINGATAN: Gagal memparsing tanggal '{record_date_str}' untuk catatan: {record}")
                        continue
        except Exception as e:
            logger.error(f"Error saat mengambil data ringkasan dari GSheets: {e}", exc_info=True)
            return None, None # Kembalikan None jika ada error
    return total_pemasukan, total_pengeluaran

# --- Fungsi-fungsi Bot Telegram ---

async def start(update: Update, context):
    """Mengirim pesan sambutan dan menampilkan pilihan kategori utama."""
    keyboard = []
    for i in range(0, len(MAIN_CATEGORIES), 2):
        row = []
        row.append(InlineKeyboardButton(MAIN_CATEGORIES[i], callback_data=f"main_category_{MAIN_CATEGORIES[i]}"))
        if i + 1 < len(MAIN_CATEGORIES):
            row.append(InlineKeyboardButton(MAIN_CATEGORIES[i+1], callback_data=f"main_category_{MAIN_CATEGORIES[i+1]}"))
        keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Halo! Saya adalah bot Telegram laporan keuangan pribadi Anda. "
        "Saya akan membantu Anda mencatat dan melacak transaksi. "
        "Silakan pilih kategori utama untuk memulai:",
        reply_markup=reply_markup
    )
    
    return CHOOSING_MAIN_CATEGORY

async def choose_main_category(update: Update, context):
    """Menangani pilihan kategori utama dan menampilkan sub kategori atau meminta jumlah."""
    query = update.callback_query
    await query.answer()

    main_category = query.data.split('_')[2]
    context.user_data['main_category'] = main_category

    sub_categories_map = {
        'Penghasilan': PENGHASILAN_SUB_CATEGORIES,
        'Pengeluaran': PENGELUARAN_SUB_CATEGORIES,
        'Tagihan': TAGIHAN_SUB_CATEGORIES,
        'Hutang': HUTANG_SUB_CATEGORIES,
        'Investasi': INVESTASI_SUB_CATEGORIES
    }
    sub_categories_list = sub_categories_map.get(main_category, [])
    
    if sub_categories_list:
        keyboard = []
        for i in range(0, len(sub_categories_list), 2):
            row = []
            row.append(InlineKeyboardButton(sub_categories_list[i], callback_data=f"sub_category_{sub_categories_list[i]}"))
            if i + 1 < len(sub_categories_list):
                row.append(InlineKeyboardButton(sub_categories_list[i+1], callback_data=f"sub_category_{sub_categories_list[i+1]}"))
            keyboard.append(row)
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"Anda memilih kategori: *{main_category}*. Silakan pilih sub kategori:", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        return CHOOSING_SUB_CATEGORY
    else:
        context.user_data['sub_category_chosen'] = ""
        await query.edit_message_text(f"Anda memilih kategori: *{main_category}*. Berapa jumlahnya? (contoh: 50000)", parse_mode=ParseMode.MARKDOWN)
        return ASKING_AMOUNT

async def choose_sub_category(update: Update, context):
    """Menangani pilihan sub kategori dan meminta jumlah."""
    query = update.callback_query
    await query.answer()

    sub_category_chosen = query.data.split('_')[2]
    context.user_data['sub_category_chosen'] = sub_category_chosen

    await query.edit_message_text(f"Anda memilih sub kategori: *{sub_category_chosen}*. Berapa jumlahnya? (contoh: 50000)", parse_mode=ParseMode.MARKDOWN)
    return ASKING_AMOUNT

async def ask_amount(update: Update, context):
    """Meminta jumlah transaksi dan kemudian menampilkan pilihan posisi kas."""
    text = update.message.text
    try:
        amount = int(text)
        if amount <= 0:
            await update.message.reply_text("Jumlah harus angka positif. Berapa jumlahnya?")
            return ASKING_AMOUNT
        context.user_data['amount'] = amount
        
        keyboard = []
        for i in range(0, len(POSISI_KAS_OPTIONS), 2):
            row = []
            row.append(InlineKeyboardButton(POSISI_KAS_OPTIONS[i], callback_data=f"posisi_{POSISI_KAS_OPTIONS[i]}"))
            if i + 1 < len(POSISI_KAS_OPTIONS):
                row.append(InlineKeyboardButton(POSISI_KAS_OPTIONS[i+1], callback_data=f"posisi_{POSISI_KAS_OPTIONS[i+1]}"))
            keyboard.append(row)
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text("Dari mana sumber/tujuan kas ini? (Posisi Kas)", reply_markup=reply_markup)
        return ASKING_POSISI_KAS

    except ValueError:
        await update.message.reply_text("Jumlah harus berupa angka. Berapa jumlahnya?")
        return ASKING_AMOUNT

async def ask_posisi_kas(update: Update, context):
    """Menangani pilihan posisi kas dan meminta keterangan."""
    query = update.callback_query
    await query.answer()

    posisi_kas = query.data.split('_')[1]
    context.user_data['posisi_kas'] = posisi_kas

    await query.edit_message_text(f"Anda memilih posisi kas: *{posisi_kas}*. Berikan keterangan singkat: (contoh: Bisnis kopi)", parse_mode=ParseMode.MARKDOWN)
    return ASKING_KETERANGAN

async def ask_keterangan(update: Update, context):
    """Meminta keterangan transaksi dan menyimpan data ke Google Sheets."""
    context.user_data['keterangan'] = update.message.text
    user_id = update.effective_user.id
    
    main_category = context.user_data.get('main_category')
    sub_category_chosen = context.user_data.get('sub_category_chosen', "")
    amount = context.user_data.get('amount')
    posisi_kas = context.user_data.get('posisi_kas')
    keterangan = context.user_data.get('keterangan')
    # Mengubah format tanggal menjadi DD/MM/YY
    tanggal = datetime.now().strftime("%d/%m/%y") 
    
    # Hasilkan ID transaksi unik
    transaction_id = f"{user_id}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    uang_masuk_value = 0
    uang_keluar_value = 0
    if main_category == 'Penghasilan':
        uang_masuk_value = amount
    else: # Pengeluaran, Tagihan, Hutang, Investasi
        uang_keluar_value = amount

    if worksheet:
        try:
            # Dapatkan semua nilai dari sheet untuk membaca header
            all_sheet_values = worksheet.get_all_values()
            if len(all_sheet_values) < 3:
                await update.message.reply_text("Struktur Google Sheet tidak valid. Baris header (baris 3) tidak ditemukan.")
                logger.error("Google Sheet memiliki kurang dari 3 baris, tidak dapat menemukan header.")
                context.user_data.clear()
                return ConversationHandler.END

            headers = all_sheet_values[2] # Header berada di baris ke-3 (indeks 2)
            
            # Buat peta dari nama header ke posisi kolom berindeks 0
            header_to_col_index = {header.strip(): i for i, header in enumerate(headers)}
            logger.info(f"DEBUG: Raw headers from sheet row 3: {headers}") # Log tambahan
            logger.info(f"DEBUG: Cleaned headers (stripped): {[h.strip() for h in headers]}") # Log tambahan
            logger.info(f"DEBUG: Header to column index map: {header_to_col_index}") # Log tambahan

            # Tentukan kolom yang diharapkan bot untuk ditulis dan nilai-nilainya
            # PENTING: Nama header ini harus cocok persis dengan header di Google Sheet Anda (case-sensitive)
            data_to_write_map = {
                'TANGGAL': tanggal,
                'KATEGORI': main_category,
                'SUB KATEGORI': sub_category_chosen,
                'UANG MASUK': uang_masuk_value,
                'UANG KELUAR': uang_keluar_value,
                'POSISI KAS': posisi_kas,
                'KETERANGAN': keterangan,
                'USER ID': str(user_id),
                'TRANSACTION ID': transaction_id
            }
            logger.info(f"DEBUG: Data to write map (expected values): {data_to_write_map}") # Log tambahan

            # Find the maximum column index that we actually intend to write to
            # This ensures data_row is large enough for all *expected* columns
            max_target_col_idx = -1
            for header_name in data_to_write_map.keys():
                if header_name in header_to_col_index:
                    max_target_col_idx = max(max_target_col_idx, header_to_col_index[header_name])
            
            if max_target_col_idx == -1:
                await update.message.reply_text("Tidak ada kolom header yang dikenal ditemukan di Google Sheet Anda. Tidak dapat mencatat transaksi.")
                logger.error("No known column headers found in Google Sheet. Cannot append row.")
                context.user_data.clear()
                return ConversationHandler.END

            # Initialize data_row with enough empty strings up to the last target column
            # Plus one because col_idx is 0-based
            data_row = [''] * (max_target_col_idx + 1) 
            logger.info(f"DEBUG: Initialized data_row length: {len(data_row)} (max_target_col_idx={max_target_col_idx})")

            # Isi data_row berdasarkan posisi header yang ditemukan
            for header_name, value in data_to_write_map.items():
                if header_name in header_to_col_index:
                    col_idx = header_to_col_index[header_name]
                    data_row[col_idx] = value
                else:
                    logger.warning(f"Header '{header_name}' tidak ditemukan di sheet. Data untuk bidang ini tidak akan ditulis.")
                    # Jika header kritis tidak ditemukan, beri tahu pengguna
                    if header_name in ['TANGGAL', 'KATEGORI', 'UANG MASUK', 'UANG KELUAR', 'POSISI KAS', 'KETERANGAN', 'USER ID', 'TRANSACTION ID']:
                        await update.message.reply_text(f"Peringatan: Kolom '{header_name}' tidak ditemukan di Google Sheet Anda. Pastikan semua header kolom sudah benar di baris ke-3.")
                        logger.error(f"Missing critical column header: '{header_name}' in Google Sheet. Headers found: {headers}")
                        context.user_data.clear()
                        return ConversationHandler.END
            
            logger.info(f"DEBUG: Data prepared for append: {data_row}") # Log tambahan

            message_id = (await update.message.reply_text("Sedang mencatat transaksi ke Google Sheets...")).message_id
            worksheet.append_row(data_row)
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=f"Transaksi *{main_category}* sebesar *{format_rupiah(amount)}* ({sub_category_chosen}) berhasil dicatat di Google Sheets.\n"
                     f"ID Transaksi Anda: `{transaction_id}`",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Transaksi dicatat: {data_row} oleh user {user_id}")
        except Exception as e:
            await update.message.reply_text(f"Gagal mencatat ke Google Sheets: {e}. Data tidak disimpan secara permanen.")
            logger.error(f"Error saat menulis ke GSheets: {e}", exc_info=True)
    else:
        await update.message.reply_text(f"Bot belum terhubung ke Google Sheets. Transaksi *{main_category}* sebesar *{format_rupiah(amount)}* ({sub_category_chosen}) tidak dapat disimpan secara permanen.", parse_mode=ParseMode.MARKDOWN)
        logger.warning(f"Percobaan mencatat transaksi tanpa koneksi GSheets: {data_row} oleh user {user_id}")
    
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context):
    """Membatalkan alur percakapan."""
    await update.message.reply_text("Percakapan dibatalkan.")
    context.user_data.clear()
    return ConversationHandler.END

async def ringkasan_hari(update: Update, context):
    """Menampilkan ringkasan pemasukan dan pengeluaran untuk hari ini."""
    user_id = update.effective_user.id
    today = datetime.now().date()
    
    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menampilkan ringkasan.")
        return

    message_id = (await update.message.reply_text("Mengambil ringkasan harian...")).message_id
    pemasukan, pengeluaran = await get_summary_data(user_id, today, today)

    if pemasukan is not None and pengeluaran is not None:
        pesan = f"Ringkasan Keuangan Hari Ini ({today.strftime('%d %B %Y')}):\n" 
        pesan += f"Pemasukan: *{format_rupiah(pemasukan)}*\n"
        pesan += f"Pengeluaran: *{format_rupiah(pengeluaran)}*\n"
        pesan += f"Saldo Bersih: *{format_rupiah(pemasukan - pengeluaran)}*"
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text=pesan,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text="Gagal mendapatkan ringkasan harian. Mohon coba lagi nanti."
        )

async def ringkasan_minggu(update: Update, context):
    """Menampilkan ringkasan pemasukan dan pengeluaran untuk minggu ini."""
    user_id = update.effective_user.id
    today = datetime.now().date()
    
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)

    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menampilkan ringkasan.")
        return

    message_id = (await update.message.reply_text(f"Mengambil ringkasan mingguan ({start_of_week.strftime('%d %B %Y')} s/d {end_of_week.strftime('%d %B %Y')})...")).message_id 
    pemasukan, pengeluaran = await get_summary_data(user_id, start_of_week, end_of_week)

    if pemasukan is not None and pengeluaran is not None:
        pesan = f"Ringkasan Keuangan Keuangan Minggu Ini ({start_of_week.strftime('%d %B %Y')} - {end_of_week.strftime('%d %B %Y')}):\n" 
        pesan += f"Pemasukan: *{format_rupiah(pemasukan)}*\n"
        pesan += f"Pengeluaran: *{format_rupiah(pengeluaran)}*\n"
        pesan += f"Saldo Bersih: *{format_rupiah(pemasukan - pengeluaran)}*"
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text=pesan,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text="Gagal mendapatkan ringkasan mingguan. Mohon coba lagi nanti."
        )

async def ringkasan_bulan(update: Update, context):
    """Menampilkan ringkasan pemasukan dan pengeluaran untuk bulan ini."""
    user_id = update.effective_user.id
    today = datetime.now().date()
    
    start_of_month = today.replace(day=1)
    end_of_month = today.replace(day=calendar.monthrange(today.year, today.month)[1])

    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menampilkan ringkasan.")
        return

    message_id = (await update.message.reply_text(f"Mengambil ringkasan bulanan ({start_of_month.strftime('%d %B %Y')} s/d {end_of_month.strftime('%d %B %Y')})...")).message_id 
    pemasukan, pengeluaran = await get_summary_data(user_id, start_of_month, end_of_month)

    if pemasukan is not None and pengeluaran is not None:
        pesan = f"Ringkasan Keuangan Bulan Ini ({start_of_month.strftime('%B %Y')}):\n"
        pesan += f"Pemasukan: *{format_rupiah(pemasukan)}*\n"
        pesan += f"Pengeluaran: *{format_rupiah(pengeluaran)}*\n"
        pesan += f"Saldo Bersih: *{format_rupiah(pemasukan - pengeluaran)}*"
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text=pesan,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text="Gagal mendapatkan ringkasan bulanan. Mohon coba lagi nanti."
        )

async def export_data(update: Update, context):
    """Mengekspor data transaksi dari Google Sheets ke file CSV dan mengirimkannya ke pengguna."""
    user_id = update.effective_user.id
    
    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat mengekspor data.")
        return

    message_id_obj = (await update.message.reply_text("Mengekspor data transaksi Anda. Mohon tunggu...")).message_id

    try:
        all_data_raw = worksheet.get_all_values()

        if not all_data_raw:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id_obj,
                text="Tidak ada data transaksi yang ditemukan di Google Sheets untuk diekspor."
            )
            return

        output = io.StringIO()
        writer = csv.writer(output)

        # Asumsi header berada di baris 3 (indeks 2)
        headers = all_data_raw[2] 
        # Bersihkan header (hapus spasi) dan tangani potensi header kosong
        cleaned_headers = [h.strip() if h else f"EMPTY_HEADER_{i}" for i, h in enumerate(headers)]
        writer.writerow(cleaned_headers) # Tulis header yang sudah dibersihkan ke CSV

        # Data dimulai dari baris 4 (indeks 3)
        data_rows = all_data_raw[3:]

        # Temukan indeks untuk 'UANG MASUK' dan 'UANG KELUAR' berdasarkan cleaned_headers (diubah menjadi huruf kapital)
        uang_masuk_idx = -1
        uang_keluar_idx = -1
        try:
            uang_masuk_idx = cleaned_headers.index('UANG MASUK') 
            uang_keluar_idx = cleaned_headers.index('UANG KELUAR') 
        except ValueError:
            logger.error("Kolom 'UANG MASUK' atau 'UANG KELUAR' tidak ditemukan di header Google Sheet. Ekspor mungkin tidak akurat.")

        for row_values in data_rows:
            cleaned_row = list(row_values) # Konversi tuple ke daftar untuk modifikasi
            
            # Pastikan baris memiliki cukup elemen sebelum mengakses
            if uang_masuk_idx != -1 and len(cleaned_row) > uang_masuk_idx:
                cleaned_row[uang_masuk_idx] = clean_numeric_string(cleaned_row[uang_masuk_idx])
            
            if uang_keluar_idx != -1 and len(cleaned_row) > uang_keluar_idx:
                cleaned_row[uang_keluar_idx] = clean_numeric_string(cleaned_row[uang_keluar_idx])
            
            writer.writerow(cleaned_row)
        
        output.seek(0)
        
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=io.BytesIO(output.getvalue().encode('utf-8')),
            filename=f"transaksi_keuangan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            caption="Berikut adalah data transaksi keuangan Anda dalam format CSV."
        )
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id_obj,
            text="File CSV berhasil dikirim!"
        )

    except Exception as e:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id_obj,
            text=f"Gagal mengekspor data: {e}. Mohon coba lagi nanti."
        )
        logger.error(f"Error saat mengekspor data: {e}", exc_info=True)

# --- FUNGSI HAPUS TRANSAKSI ---
async def hapus_transaksi_start(update: Update, context):
    """Memulai alur penghapusan transaksi."""
    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menghapus transaksi.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Untuk menghapus transaksi, silakan masukkan *Transaction ID* transaksi yang ingin Anda hapus.\n"
        "Jika Anda tidak tahu ID-nya, ketik `list` untuk melihat 10 transaksi terakhir Anda.",
        parse_mode=ParseMode.MARKDOWN
    )
    return DELETE_ASKING_ID

async def list_user_transactions(update: Update, context):
    """Membantu pengguna melihat transaksi terakhir mereka dengan ID."""
    user_id = update.effective_user.id
    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menampilkan transaksi.")
        return

    try:
        # Gunakan fungsi header kustom
        all_records = get_records_with_custom_header(worksheet)
        user_transactions = [
            record for record in all_records
            if str(record.get('USER ID')) == str(user_id) and record.get('TRANSACTION ID') 
        ]
        
        if not user_transactions:
            await update.message.reply_text("Anda belum mencatat transaksi apa pun yang memiliki ID transaksi.")
            return

        # Urutkan berdasarkan tanggal dan kemudian ID transaksi (bagian timestamp) untuk mendapatkan yang terbaru secara handal
        user_transactions.sort(key=lambda x: (x.get('TANGGAL', '0000-0000'), x.get('TRANSACTION ID', '')), reverse=True) 

        message_text = "10 Transaksi Terakhir Anda (dengan ID):\n\n"
        for i, item in enumerate(user_transactions[:10]): # Batasi hingga 10 terakhir
            tanggal = item.get('TANGGAL', 'N/A') 
            kategori = item.get('KATEGORI', 'N/A') 
            sub_kategori = item.get('SUB KATEGORI', '') 
            uang_masuk = format_rupiah(clean_numeric_string(item.get('UANG MASUK', 0))) 
            uang_keluar = format_rupiah(clean_numeric_string(item.get('UANG KELUAR', 0))) 
            keterangan = item.get('KETERANGAN', 'N/A')
            trans_id = item.get('TRANSACTION ID', 'N/A') 

            jumlah_tampil = f"Masuk: {uang_masuk}" if clean_numeric_string(item.get('UANG MASUK', 0)) > 0 else f"Keluar: {uang_keluar}" 

            message_text += (
                f"{i+1}. Tanggal: {tanggal}\n"
                f"   Kategori: {kategori} ({sub_kategori})\n"
                f"   Jumlah: {jumlah_tampil}\n"
                f"   Keterangan: {keterangan}\n"
                f"   ID: `{trans_id}`\n\n"
            )
        
        await update.message.reply_text(message_text, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await update.message.reply_text(f"Gagal mengambil daftar transaksi: {e}. Mohon coba lagi nanti.")
        logger.error(f"Error saat menampilkan daftar transaksi: {e}", exc_info=True)


async def hapus_transaksi_get_id(update: Update, context):
    """Menerima ID transaksi untuk dihapus atau perintah 'list'."""
    user_input = update.message.text.strip().lower()
    user_id = update.effective_user.id

    if user_input == 'list':
        await list_user_transactions(update, context)
        await update.message.reply_text("Silakan masukkan *Transaction ID* dari daftar di atas, atau ketik `cancel` untuk membatalkan.", parse_mode=ParseMode.MARKDOWN)
        return DELETE_ASKING_ID

    transaction_id_to_delete = user_input
    
    try:
        # Gunakan fungsi header kustom
        all_records = get_records_with_custom_header(worksheet)
        found_transaction = None
        row_index_to_delete = -1

        # Temukan transaksi berdasarkan Transaction ID dan User ID
        # Daftar catatan adalah berindeks 0, dan data dimulai dari baris sheet 4 (header adalah baris 3)
        # Jadi, sheet_row = list_index + header_row_index_1based + 1 = list_index + 3 + 1 = list_index + 4
        for i, record in enumerate(all_records):
            if str(record.get('USER ID')) == str(user_id) and record.get('TRANSACTION ID') == transaction_id_to_delete: 
                found_transaction = record
                row_index_to_delete = i + 4 # Indeks baris yang disesuaikan untuk sheet
                break
        
        if found_transaction:
            context.user_data['transaction_to_delete'] = found_transaction
            context.user_data['row_index_to_delete'] = row_index_to_delete

            tanggal = found_transaction.get('TANGGAL', 'N/A') 
            kategori = found_transaction.get('KATEGORI', 'N/A') 
            sub_kategori = found_transaction.get('SUB KATEGORI', '') 
            uang_masuk = format_rupiah(clean_numeric_string(found_transaction.get('UANG MASUK', 0))) 
            uang_keluar = format_rupiah(clean_numeric_string(found_transaction.get('UANG KELUAR', 0))) 
            keterangan = found_transaction.get('KETERANGAN', 'N/A')
            
            jumlah_tampil = f"Masuk: {uang_masuk}" if clean_numeric_string(found_transaction.get('UANG MASUK', 0)) > 0 else f"Keluar: {uang_keluar}" 

            confirmation_message = (
                f"Anda yakin ingin menghapus transaksi ini?\n\n"
                f"Tanggal: {tanggal}\n"
                f"Kategori: {kategori} ({sub_kategori})\n"
                f"Jumlah: {jumlah_tampil}\n"
                f"Keterangan: {keterangan}\n"
                f"ID: `{transaction_id_to_delete}`\n\n"
                f"Ketik *'ya'* untuk mengonfirmasi, atau *'tidak'* untuk membatalkan."
            )
            await update.message.reply_text(confirmation_message, parse_mode=ParseMode.MARKDOWN)
            return DELETE_CONFIRMATION
        else:
            await update.message.reply_text(
                "Transaction ID tidak ditemukan atau bukan milik Anda. "
                "Mohon masukkan ID yang valid, atau ketik `list` untuk melihat transaksi Anda, atau `cancel` untuk membatalkan.",
                parse_mode=ParseMode.MARKDOWN
            )
            return DELETE_ASKING_ID
    
    except Exception as e:
        await update.message.reply_text(f"Terjadi kesalahan saat mencari transaksi: {e}. Mohon coba lagi nanti.")
        logger.error(f"Error saat mencari transaksi untuk dihapus: {e}", exc_info=True)
        context.user_data.clear()
        return ConversationHandler.END

async def hapus_transaksi_confirm(update: Update, context):
    """Mengonfirmasi penghapusan transaksi."""
    user_response = update.message.text.strip().lower()

    if user_response == 'ya':
        row_index = context.user_data.get('row_index_to_delete')
        transaction_id = context.user_data.get('transaction_to_delete', {}).get('TRANSACTION ID') 

        if row_index and worksheet:
            try:
                worksheet.delete_rows(row_index)
                await update.message.reply_text(f"Transaksi dengan ID `{transaction_id}` berhasil dihapus.")
                logger.info(f"Transaksi dengan ID {transaction_id} dihapus oleh user {update.effective_user.id}")
            except Exception as e:
                await update.message.reply_text(f"Gagal menghapus transaksi dari Google Sheets: {e}. Mohon coba lagi nanti.")
                logger.error(f"Error saat menghapus baris dari GSheets: {e}", exc_info=True)
        else:
            await update.message.reply_text("Terjadi kesalahan. Transaksi tidak dapat dihapus.")
    elif user_response == 'tidak':
        await update.message.reply_text("Penghapusan transaksi dibatalkan.")
    else:
        await update.message.reply_text("Respon tidak valid. Ketik *'ya'* untuk mengonfirmasi, atau *'tidak'* untuk membatalkan.", parse_mode=ParseMode.MARKDOWN)
        return DELETE_CONFIRMATION # Tetap dalam status ini sampai respons valid

    context.user_data.clear()
    return ConversationHandler.END

# --- FUNGSI EDIT TRANSAKSI ---
async def edit_transaksi_start(update: Update, context):
    """Memulai alur pengeditan transaksi."""
    if not worksheet:
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat mengedit transaksi.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Untuk mengedit transaksi, silakan masukkan *Transaction ID* transaksi yang ingin Anda edit.\n"
        "Jika Anda tidak tahu ID-nya, ketik `list` untuk melihat 10 transaksi terakhir Anda.",
        parse_mode=ParseMode.MARKDOWN
    )
    return EDIT_ASKING_ID

async def edit_transaksi_get_id(update: Update, context):
    """Menerima ID transaksi untuk diedit atau perintah 'list'."""
    user_input = update.message.text.strip().lower()
    user_id = update.effective_user.id

    if user_input == 'list':
        await list_user_transactions(update, context) # Gunakan kembali fungsi daftar yang ada
        await update.message.reply_text("Silakan masukkan *Transaction ID* dari daftar di atas, atau ketik `cancel` untuk membatalkan.", parse_mode=ParseMode.MARKDOWN)
        return EDIT_ASKING_ID

    transaction_id_to_edit = user_input
    
    try:
        # Gunakan fungsi header kustom
        all_records = get_records_with_custom_header(worksheet)
        found_transaction = None
        row_index_to_edit = -1

        # Temukan transaksi berdasarkan Transaction ID dan User ID
        # Daftar catatan adalah berindeks 0, dan data dimulai dari baris sheet 4 (header adalah baris 3)
        # Jadi, sheet_row = list_index + header_row_index_1based + 1 = list_index + 3 + 1 = list_index + 4
        for i, record in enumerate(all_records):
            if str(record.get('USER ID')) == str(user_id) and record.get('TRANSACTION ID') == transaction_id_to_edit: 
                found_transaction = record
                row_index_to_edit = i + 4 # Indeks baris yang disesuaikan untuk sheet
                break
        
        if found_transaction:
            context.user_data['transaction_to_edit'] = found_transaction
            context.user_data['row_index_to_edit'] = row_index_to_edit

            # Tampilkan detail transaksi saat ini
            tanggal = found_transaction.get('TANGGAL', 'N/A') 
            kategori = found_transaction.get('KATEGORI', 'N/A') 
            sub_kategori = found_transaction.get('SUB KATEGORI', '') 
            uang_masuk = format_rupiah(clean_numeric_string(found_transaction.get('UANG MASUK', 0))) 
            uang_keluar = format_rupiah(clean_numeric_string(found_transaction.get('UANG KELUAR', 0))) 
            posisi_kas = found_transaction.get('POSISI KAS', 'N/A') 
            keterangan = found_transaction.get('KETERANGAN', 'N/A') 
            
            current_details_message = (
                f"Detail Transaksi:\n"
                f"Tanggal: *{tanggal}*\n"
                f"Kategori: *{kategori}* ({sub_kategori})\n"
                f"Uang Masuk: *{uang_masuk}*\n"
                f"Uang Keluar: *{uang_keluar}*\n"
                f"Posisi Kas: *{posisi_kas}*\n"
                f"Keterangan: *{keterangan}*\n"
                f"ID: `{transaction_id_to_edit}`\n\n"
                f"Bidang mana yang ingin Anda edit?"
            )

            keyboard = [
                [InlineKeyboardButton("Tanggal", callback_data="edit_field_TANGGAL")], 
                [InlineKeyboardButton("Kategori", callback_data="edit_field_KATEGORI")], 
                [InlineKeyboardButton("Sub Kategori", callback_data="edit_field_SUB KATEGORI")], 
                [InlineKeyboardButton("Uang Masuk", callback_data="edit_field_UANG MASUK")], 
                [InlineKeyboardButton("Uang Keluar", callback_data="edit_field_UANG KELUAR")], 
                [InlineKeyboardButton("Posisi Kas", callback_data="edit_field_POSISI KAS")],
                [InlineKeyboardButton("Keterangan", callback_data="edit_field_KETERANGAN")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(current_details_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            return EDIT_CHOOSING_FIELD
        else:
            await update.message.reply_text(
                "Transaction ID tidak ditemukan atau bukan milik Anda. "
                "Mohon masukkan ID yang valid, atau ketik `list` untuk melihat transaksi Anda, atau `cancel` untuk membatalkan.",
                parse_mode=ParseMode.MARKDOWN
            )
            return EDIT_ASKING_ID
    
    except Exception as e:
        await update.message.reply_text(f"Terjadi kesalahan saat mencari transaksi: {e}. Mohon coba lagi nanti.")
        logger.error(f"Error saat mencari transaksi untuk diedit: {e}", exc_info=True)
        context.user_data.clear()
        return ConversationHandler.END

async def edit_transaksi_choose_field(update: Update, context):
    """Menangani pilihan bidang yang akan diedit."""
    query = update.callback_query
    await query.answer()

    field_to_edit = query.data.split('_')[2]
    context.user_data['field_to_edit'] = field_to_edit

    # Dapatkan nilai saat ini untuk konteks
    current_transaction = context.user_data.get('transaction_to_edit', {})
    current_value = current_transaction.get(field_to_edit, 'N/A')

    if field_to_edit in ['UANG MASUK', 'UANG KELUAR']: 
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{format_rupiah(clean_numeric_string(current_value))}*.\nMasukkan nilai baru (hanya angka):", parse_mode=ParseMode.MARKDOWN)
    elif field_to_edit == 'TANGGAL': 
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{current_value}*.\nMasukkan tanggal baru (contoh: 28/05/25):", parse_mode=ParseMode.MARKDOWN) # Contoh diubah
    elif field_to_edit == 'KATEGORI': 
        keyboard = []
        for i in range(0, len(MAIN_CATEGORIES), 2): 
            row = []
            row.append(InlineKeyboardButton(MAIN_CATEGORIES[i], callback_data=f"edit_new_value_KATEGORI_{MAIN_CATEGORIES[i]}")) 
            if i + 1 < len(MAIN_CATEGORIES):
                row.append(InlineKeyboardButton(MAIN_CATEGORIES[i+1], callback_data=f"edit_new_value_KATEGORI_{MAIN_CATEGORIES[i+1]}")) 
            keyboard.append(row)
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{current_value}*.\nSilakan pilih kategori baru:", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        return EDIT_ASKING_NEW_VALUE 
    elif field_to_edit == 'SUB KATEGORI': 
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{current_value}*.\nMasukkan sub kategori baru:", parse_mode=ParseMode.MARKDOWN)
    elif field_to_edit == 'POSISI KAS': 
        keyboard = []
        for i in range(0, len(POSISI_KAS_OPTIONS), 2):
            row = []
            row.append(InlineKeyboardButton(POSISI_KAS_OPTIONS[i], callback_data=f"edit_new_value_POSISI KAS_{POSISI_KAS_OPTIONS[i]}"))
            if i + 1 < len(POSISI_KAS_OPTIONS):
                row.append(InlineKeyboardButton(POSISI_KAS_OPTIONS[i+1], callback_data=f"edit_new_value_POSISI KAS_{POSISI_KAS_OPTIONS[i+1]}"))
            keyboard.append(row)
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{current_value}*.\nSilakan pilih posisi kas baru:", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        return EDIT_ASKING_NEW_VALUE 
    else: # Keterangan, atau bidang teks lainnya
        await query.edit_message_text(f"Anda memilih untuk mengedit *{field_to_edit}*. Nilai saat ini: *{current_value}*.\nMasukkan nilai baru:", parse_mode=ParseMode.MARKDOWN)
    
    return EDIT_ASKING_NEW_VALUE

async def edit_transaksi_get_new_value(update: Update, context):
    """Menerima nilai baru untuk bidang yang diedit."""
    field_to_edit = context.user_data.get('field_to_edit')
    old_transaction = context.user_data.get('transaction_to_edit')
    old_value = old_transaction.get(field_to_edit, 'N/A')

    new_value_input = None
    if update.message:
        new_value_input = update.message.text 
    elif update.callback_query:
        query = update.callback_query
        await query.answer()
        parts = query.data.split('_')
        if len(parts) >= 4 and parts[0] == 'edit' and parts[1] == 'new' and parts[2] == 'value':
            field_to_edit_from_callback = parts[3]
            new_value_input = "_".join(parts[4:]) 
            context.user_data['field_to_edit'] = field_to_edit_from_callback 
            field_to_edit = field_to_edit_from_callback
        else:
            await query.edit_message_text("Pilihan tidak valid. Silakan coba lagi atau ketik `cancel`.", parse_mode=ParseMode.MARKDOWN)
            return EDIT_ASKING_NEW_VALUE 

    if new_value_input is None: 
        if update.callback_query:
            await update.callback_query.edit_message_text("Input tidak valid. Silakan coba lagi atau ketik `cancel`.", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("Input tidak valid. Silakan coba lagi atau ketik `cancel`.", parse_mode=ParseMode.MARKDOWN)
        return EDIT_ASKING_NEW_VALUE

    new_value_processed = new_value_input 

    # Validasi dan konversi tipe
    if field_to_edit in ['UANG MASUK', 'UANG KELUAR']: 
        try:
            new_value_processed = int(clean_numeric_string(new_value_input)) 
            if new_value_processed < 0:
                if update.callback_query:
                    await update.callback_query.edit_message_text("Jumlah harus angka positif. Masukkan nilai baru:", parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.message.reply_text("Jumlah harus angka positif. Masukkan nilai baru:", parse_mode=ParseMode.MARKDOWN)
                return EDIT_ASKING_NEW_VALUE
        except ValueError:
            if update.callback_query:
                await update.callback_query.edit_message_text("Jumlah harus berupa angka. Masukkan nilai baru:", parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text("Jumlah harus berupa angka. Masukkan nilai baru:", parse_mode=ParseMode.MARKDOWN)
            return EDIT_ASKING_NEW_VALUE
    elif field_to_edit == 'TANGGAL': 
        try:
            # Validasi format tanggal baru DD/MM/YY
            datetime.strptime(new_value_input, "%d/%m/%y") 
            new_value_processed = new_value_input # Tetap sebagai string jika tanggal valid
        except ValueError:
            if update.callback_query:
                await update.callback_query.edit_message_text("Format tanggal tidak valid (contoh: 28/05/25). Masukkan tanggal baru:", parse_mode=ParseMode.MARKDOWN) 
            else:
                await update.message.reply_text("Format tanggal tidak valid (contoh: 28/05/25). Masukkan tanggal baru:", parse_mode=ParseMode.MARKDOWN) 
            return EDIT_ASKING_NEW_VALUE
    
    context.user_data['new_value'] = new_value_processed

    # Siapkan nilai untuk ditampilkan di pesan konfirmasi
    display_old_value = old_value
    if field_to_edit in ['UANG MASUK', 'UANG KELUAR']: 
        display_old_value = format_rupiah(clean_numeric_string(old_value))
    
    display_new_value = new_value_processed
    if field_to_edit in ['UANG MASUK', 'UANG KELUAR']: 
        display_new_value = format_rupiah(new_value_processed)


    confirmation_message = (
        f"Anda ingin mengubah bidang *{field_to_edit}* dari transaksi ini:\n"
        f"Nilai Lama: *{display_old_value}*\n"
        f"Nilai Baru: *{display_new_value}*\n\n"
        f"Ketik *'ya'* untuk mengonfirmasi, atau *'tidak'* untuk membatalkan."
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(confirmation_message, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(confirmation_message, parse_mode=ParseMode.MARKDOWN)
    
    return EDIT_CONFIRMATION

async def edit_transaksi_confirm(update: Update, context):
    """Mengonfirmasi dan menerapkan perubahan transaksi."""
    user_response = update.message.text.strip().lower()

    if user_response == 'ya':
        row_index = context.user_data.get('row_index_to_edit')
        field_to_edit = context.user_data.get('field_to_edit')
        new_value = context.user_data.get('new_value')
        transaction_id = context.user_data.get('transaction_to_edit', {}).get('TRANSACTION ID') 

        if row_index and worksheet:
            try:
                # Dapatkan semua nilai dari sheet untuk membaca header
                all_values = worksheet.get_all_values()
                headers = all_values[2] # Baris 3 adalah indeks 2
                
                # Buat peta dari nama header ke posisi kolom berindeks 0
                header_to_col_index = {header.strip(): i for i, header in enumerate(headers)}
                logger.info(f"Peta header ke indeks kolom untuk edit: {header_to_col_index}")

                # Petakan field_to_edit ke nama kolom sebenarnya di Google Sheet
                # PENTING: Ini harus cocok persis dengan header Google Sheet Anda (case-sensitive)
                # Gunakan dictionary ini untuk memastikan nama yang benar saat mencari index
                column_name_map = {
                    'Tanggal': 'TANGGAL', 
                    'Kategori': 'KATEGORI', 
                    'Sub Kategori': 'SUB KATEGORI', 
                    'Uang Masuk': 'UANG MASUK', 
                    'Uang Keluar': 'UANG KELUAR', 
                    'Posisi Kas': 'POSISI KAS', 
                    'Keterangan': 'KETERANGAN',
                    'User ID': 'USER ID', # Tambahkan jika bisa diedit
                    'Transaction ID': 'TRANSACTION ID' # Tambahkan jika bisa diedit
                }
                actual_column_name = column_name_map.get(field_to_edit, field_to_edit) 

                if actual_column_name not in header_to_col_index:
                    await update.message.reply_text(f"Kesalahan: Kolom '{actual_column_name}' tidak ditemukan di Google Sheet Anda. Tidak dapat mengedit.")
                    logger.error(f"Kolom '{actual_column_name}' tidak ditemukan di header GSheet: {headers}")
                    context.user_data.clear()
                    return ConversationHandler.END

                col_index_0based = header_to_col_index[actual_column_name]
                col_index_1based = col_index_0based + 1 # gspread adalah berindeks 1 untuk kolom

                # Penanganan khusus untuk Uang Masuk/Uang Keluar jika kategori berubah
                if field_to_edit == 'KATEGORI': 
                    # Ambil data transaksi saat ini untuk mendapatkan jumlah total
                    current_transaction_data = get_records_with_custom_header(worksheet)[row_index - 4] 
                    amount_value = clean_numeric_string(current_transaction_data.get('UANG MASUK', 0)) + clean_numeric_string(current_transaction_data.get('UANG KELUAR', 0)) 

                    uang_masuk_col_idx_1based = header_to_col_index.get('UANG MASUK', -1) + 1 
                    uang_keluar_col_idx_1based = header_to_col_index.get('UANG KELUAR', -1) + 1 

                    if uang_masuk_col_idx_1based > 0 and uang_keluar_col_idx_1based > 0:
                        if new_value == 'Penghasilan':
                            worksheet.update_cell(row_index, uang_masuk_col_idx_1based, amount_value)
                            worksheet.update_cell(row_index, uang_keluar_col_idx_1based, 0)
                        else:
                            worksheet.update_cell(row_index, uang_masuk_col_idx_1based, 0)
                            worksheet.update_cell(row_index, uang_keluar_col_idx_1based, amount_value)
                    else:
                        logger.warning("Header 'UANG MASUK' atau 'UANG KELUAR' tidak ditemukan saat mengedit kategori.")
                    
                    worksheet.update_cell(row_index, col_index_1based, new_value)
                    
                    await update.message.reply_text(f"Transaksi dengan ID `{transaction_id}` berhasil diupdate. Kategori diubah menjadi *{new_value}*.", parse_mode=ParseMode.MARKDOWN)

                elif field_to_edit == 'UANG MASUK': 
                    uang_keluar_col_idx_1based = header_to_col_index.get('UANG KELUAR', -1) + 1 
                    if uang_keluar_col_idx_1based > 0:
                        worksheet.update_cell(row_index, uang_keluar_col_idx_1based, 0)
                    worksheet.update_cell(row_index, col_index_1based, new_value)
                    await update.message.reply_text(f"Transaksi dengan ID `{transaction_id}` berhasil diupdate. Uang Masuk diubah menjadi *{format_rupiah(new_value)}*.", parse_mode=ParseMode.MARKDOWN)

                elif field_to_edit == 'UANG KELUAR': 
                    uang_masuk_col_idx_1based = header_to_col_index.get('UANG MASUK', -1) + 1 
                    if uang_masuk_col_idx_1based > 0:
                        worksheet.update_cell(row_index, uang_masuk_col_idx_1based, 0)
                    worksheet.update_cell(row_index, col_index_1based, new_value)
                    await update.message.reply_text(f"Transaksi dengan ID `{transaction_id}` berhasil diupdate. Uang Keluar diubah menjadi *{format_rupiah(new_value)}*.", parse_mode=ParseMode.MARKDOWN)
                
                else:
                    worksheet.update_cell(row_index, col_index_1based, new_value)
                    await update.message.reply_text(f"Transaksi dengan ID `{transaction_id}` berhasil diupdate. Bidang *{field_to_edit}* diubah menjadi *{new_value}*.", parse_mode=ParseMode.MARKDOWN)
                
                logger.info(f"Transaksi dengan ID {transaction_id} diupdate oleh user {update.effective_user.id}. Field: {field_to_edit}, New Value: {new_value}")

            except Exception as e:
                await update.message.reply_text(f"Gagal mengupdate transaksi di Google Sheets: {e}. Mohon coba lagi nanti.")
                logger.error(f"Error saat mengupdate baris di GSheets: {e}", exc_info=True)
        else:
            await update.message.reply_text("Terjadi kesalahan. Transaksi tidak dapat diupdate.")
    elif user_response == 'tidak':
        await update.message.reply_text("Pengeditan transaksi dibatalkan.")
    else:
        await update.message.reply_text("Respon tidak valid. Ketik *'ya'* untuk mengonfirmasi, atau *'tidak'* untuk membatalkan.", parse_mode=ParseMode.MARKDOWN)
        return EDIT_CONFIRMATION 

    context.user_data.clear()
    return ConversationHandler.END

# --- FUNGSI RESET DATA ---
async def reset_data_start(update: Update, context):
    """Memulai alur reset data."""
    keyboard = [
        [InlineKeyboardButton("Ya, Hapus Semua Data", callback_data="reset_data_confirm_yes")],
        [InlineKeyboardButton("Tidak, Batalkan", callback_data="reset_data_confirm_no")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "*PERINGATAN!* Anda akan menghapus *SEMUA* data transaksi Anda. " 
        "Tindakan ini tidak dapat dibatalkan. Anda yakin ingin melanjutkan?",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    return RESET_DATA_CONFIRMATION

async def reset_data_confirm(update: Update, context):
    """Mengonfirmasi dan melakukan reset data."""
    query = update.callback_query
    await query.answer()
    user_response = query.data.split('_')[-1]
    user_id = update.effective_user.id

    if user_response == 'yes':
        if not worksheet: 
            await query.edit_message_text("Bot belum terhubung ke Google Sheets. Tidak dapat mereset data.")
            logger.error("Reset Data: Koneksi Google Sheets tidak tersedia.")
            context.user_data.clear()
            return ConversationHandler.END

        try:
            # Hapus worksheet TRANSAKSI untuk pengguna ini
            all_transaction_records = get_records_with_custom_header(worksheet) 
            transaction_rows_to_delete = []
            for i, record in enumerate(all_transaction_records):
                if str(record.get('USER ID')) == str(user_id): 
                    transaction_rows_to_delete.append(i + 4) # +4 karena data dimulai dari baris 4 (indeks 3 dalam daftar records)
            
            for row_idx in sorted(transaction_rows_to_delete, reverse=True):
                worksheet.delete_rows(row_idx)
            logger.info(f"Berhasil menghapus {len(transaction_rows_to_delete)} baris transaksi untuk user {user_id}.")

            await query.edit_message_text("Semua data transaksi Anda berhasil dihapus.") 
            logger.info(f"Semua data transaksi untuk user {user_id} berhasil direset.") 

        except Exception as e:
            await query.edit_message_text(f"Gagal mereset data: {e}. Mohon coba lagi nanti.")
            logger.error(f"Error saat mereset data untuk user {user_id}: {e}", exc_info=True)
    elif user_response == 'no':
        await query.edit_message_text("Reset data dibatalkan.")
    else:
        await query.edit_message_text("Respon tidak valid. Silakan pilih 'Ya, Hapus Semua Data' atau 'Tidak, Batalkan'.")
        return RESET_DATA_CONFIRMATION 

    context.user_data.clear()
    return ConversationHandler.END


# --- Rangkuman Keuangan (Diperbarui untuk Saldo Berjalan) ---
async def rangkuman_keuangan(update: Update, context):
    """Menampilkan rangkuman detail pemasukan dan pengeluaran berdasarkan kategori dan sub-kategori,
       serta menampilkan saldo kas dan investasi yang diperbarui secara dinamis."""
    user_id = update.effective_user.id
    logger.info(f"Rangkuman Keuangan: Memulai untuk user {user_id}")

    if not worksheet: 
        await update.message.reply_text("Bot belum terhubung ke Google Sheets. Tidak dapat menampilkan rangkuman keuangan.")
        logger.error("Rangkuman Keuangan: Koneksi Google Sheets tidak tersedia.")
        return

    message_obj = await update.message.reply_text("Sedang menyiapkan rangkuman keuangan Anda. Mohon tunggu...")

    try:
        # --- Bagian 1: Ringkasan Transaksi (Pemasukan, Pengeluaran, dll.) ---
        # Gunakan fungsi header kustom
        all_transaction_records = get_records_with_custom_header(worksheet)
        logger.info(f"Rangkuman Keuangan: Ditemukan {len(all_transaction_records)} total catatan transaksi.")
        
        summary_data = {
            'Penghasilan': {},
            'Pengeluaran': {},
            'Tagihan': {},
            'Hutang': {},
            'Investasi': {} 
        }
        
        total_overall_income = 0
        total_overall_expense = 0
        
        user_has_transactions = False

        for record in all_transaction_records:
            record_user_id = str(record.get('USER ID')) 
            if record_user_id == str(user_id):
                user_has_transactions = True
                main_category = record.get('KATEGORI') 
                sub_category = record.get('SUB KATEGORI', 'Lain-lain') 
                
                uang_masuk = clean_numeric_string(record.get('UANG MASUK', 0)) 
                uang_keluar = clean_numeric_string(record.get('UANG KELUAR', 0)) 

                if main_category in summary_data:
                    if main_category == 'Penghasilan':
                        if uang_masuk > 0:
                            summary_data[main_category][sub_category] = summary_data[main_category].get(sub_category, 0) + uang_masuk
                            total_overall_income += uang_masuk
                    else: 
                        if uang_keluar > 0:
                            summary_data[main_category][sub_category] = summary_data[main_category].get(sub_category, 0) + uang_keluar
                            total_overall_expense += uang_keluar
        
        pesan = "*Rangkuman Keuangan Keseluruhan Anda:*\n\n"

        if not user_has_transactions:
            pesan += "_Tidak ada transaksi yang dicatat._\n\n"
        else:
            for main_cat in MAIN_CATEGORIES:
                pesan += f"*{main_cat}:*\n"
                total_cat_amount = 0
                
                sorted_sub_categories = sorted(summary_data[main_cat].keys())

                if sorted_sub_categories:
                    for sub_cat in sorted_sub_categories:
                        amount = summary_data[main_cat][sub_cat]
                        if amount > 0:
                            pesan += f"  - {sub_cat}: {format_rupiah(amount)}\n"
                            total_cat_amount += amount
                else:
                    pesan += "  _Tidak ada data._\n"
                
                pesan += f"  *Total {main_cat}: {format_rupiah(total_cat_amount)}*\n\n"

            pesan += f"*Total Pemasukan Keseluruhan: {format_rupiah(total_overall_income)}*\n"
            pesan += f"*Total Pengeluaran Keseluruhan: {format_rupiah(total_overall_expense)}*\n"
            pesan += f"*Saldo Bersih Keseluruhan (dari Transaksi): {format_rupiah(total_overall_income - total_overall_expense)}*\n\n"

        # --- Bagian 2: Saldo Kas dan Investasi (Saldo Berjalan) ---
        current_cash_balances = {opt: 0 for opt in POSISI_KAS_OPTIONS}
        logger.info(f"Rangkuman Keuangan: Saldo awal kas diinisialisasi: {current_cash_balances}")

        # Proses transaksi untuk memperbarui saldo berjalan
        user_transactions_sorted = []
        for record in all_transaction_records:
            record_user_id = str(record.get('USER ID')) 
            record_date_str = record.get('TANGGAL') 
            if record_user_id == str(user_id) and record_date_str:
                try:
                    record_date = parse_date_string(record_date_str)
                    
                    raw_uang_masuk = record.get('UANG MASUK', 0) 
                    raw_uang_keluar = record.get('UANG KELUAR', 0) 
                    
                    user_transactions_sorted.append({
                        'date': record_date,
                        'kategori': record.get('KATEGORI'), 
                        'sub_kategori': record.get('SUB KATEGORI'), 
                        'uang_masuk': clean_numeric_string(raw_uang_masuk),
                        'uang_keluar': clean_numeric_string(raw_uang_keluar),
                        'posisi_kas': record.get('POSISI KAS')
                    })
                    logger.info(f"Rangkuman Keuangan: Catatan Transaksi - Pengguna '{record_user_id}', Tanggal: '{record_date_str}', Kategori: '{record.get('KATEGORI')}', Posisi Kas: '{record.get('POSISI KAS')}', Masuk Mentah: '{raw_uang_masuk}', Masuk Bersih: {clean_numeric_string(raw_uang_masuk)}, Keluar Mentah: '{raw_uang_keluar}', Keluar Bersih: {clean_numeric_string(raw_uang_keluar)}")

                except ValueError:
                    logger.warning(f"PERINGATAN: Gagal memparsing tanggal '{record_date_str}' untuk transaksi: {record}")
                    continue
        
        user_transactions_sorted.sort(key=lambda x: x['date']) 
        logger.info(f"Rangkuman Keuangan: Ditemukan {len(user_transactions_sorted)} transaksi pengguna yang diurutkan.")


        for i, transaction in enumerate(user_transactions_sorted):
            main_category = transaction['kategori']
            posisi_kas = str(transaction['posisi_kas']) if transaction['posisi_kas'] else None 
            uang_masuk = transaction['uang_masuk']
            uang_keluar = transaction['uang_keluar']

            logger.info(f"Rangkuman Keuangan: Memproses transaksi {i+1}: Kategori='{main_category}', Posisi Kas='{posisi_kas}', Masuk={uang_masuk}, Keluar={uang_keluar}") 

            # Perbarui saldo kas berdasarkan transaksi
            if posisi_kas: 
                if posisi_kas not in current_cash_balances:
                    current_cash_balances[posisi_kas] = 0 
                    logger.info(f"Rangkuman Keuangan: Menginisialisasi posisi kas baru '{posisi_kas}' ke 0 karena ditemukan di transaksi.")

                balance_before = current_cash_balances[posisi_kas]

                if main_category == 'Penghasilan':
                    current_cash_balances[posisi_kas] += uang_masuk
                    logger.info(f"Rangkuman Keuangan: Transaksi {i+1} (Penghasilan): '{posisi_kas}' berubah dari {balance_before} menjadi {current_cash_balances[posisi_kas]} (Ditambah {uang_masuk}).")
                elif main_category in ['Pengeluaran', 'Tagihan', 'Hutang', 'Investasi']: 
                    current_cash_balances[posisi_kas] -= uang_keluar
                    logger.info(f"Rangkuman Keuangan: Transaksi {i+1} (Pengeluaran/Tagihan/Hutang/Investasi): '{posisi_kas}' berubah dari {balance_before} menjadi {current_cash_balances[posisi_kas]} (Dikurangi {uang_keluar}).")
            logger.info(f"Rangkuman Keuangan: Saldo kas saat ini setelah transaksi {i+1}: {current_cash_balances}")

        pesan += "*Saldo Kas Anda Saat Ini:*\n"
        display_cash_balances = {k: v for k, v in current_cash_balances.items() if k in POSISI_KAS_OPTIONS or v != 0}
        if display_cash_balances:
            for account, balance in sorted(display_cash_balances.items()):
                pesan += f"  - {account}: {format_rupiah(balance)}\n"
        else:
            pesan += "  _Belum ada saldo kas dicatat atau transaksi terkait._\n"
        pesan += "\n"

        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_obj.message_id,
            text=pesan,
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Rangkuman keuangan berhasil dikirim untuk user {user_id}")
        logger.info(f"Rangkuman Keuangan: Saldo kas akhir: {current_cash_balances}")


    except Exception as e:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_obj.message_id,
            text=f"Gagal mendapatkan rangkuman keuangan: {e}. Mohon coba lagi nanti."
        )
        logger.error(f"Error getting overall financial summary for user {user_id}: {e}", exc_info=True)


# --- FUNGSI BANTUAN ---
async def help_command(update: Update, context):
    """Menampilkan daftar perintah yang tersedia dan penjelasannya."""
    help_text = (
        "Berikut adalah daftar perintah yang bisa Anda gunakan:\n\n"
        "*/start* - Memulai percakapan dengan bot dan mencatat transaksi baru.\n"
        "*/cancel* - Membatalkan alur pencatatan transaksi yang sedang berjalan.\n"
        "*/hapus_transaksi* - Menghapus transaksi berdasarkan ID transaksi.\n"
        "*/edit_transaksi* - Mengedit transaksi berdasarkan ID transaksi.\n"
        "*/ringkasan_hari* - Menampilkan ringkasan keuangan (pemasukan & pengeluaran) untuk hari ini.\n"
        "*/ringkasan_minggu* - Menampilkan ringkasan keuangan untuk minggu ini.\n"
        "*/ringkasan_bulan* - Menampilkan ringkasan keuangan untuk bulan ini.\n"
        "*/rangkuman_keuangan* - Menampilkan rangkuman detail pemasukan dan pengeluaran keseluruhan, termasuk saldo kas terkini.\n" 
        "*/export_data* - Mengekspor semua data transaksi Anda ke file CSV.\n"
        "*/reset_data* - *PERINGATAN!* Menghapus semua data transaksi Anda. Tindakan ini tidak dapat dibatalkan.\n" 
        "*/help* - Menampilkan pesan bantuan ini."
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def echo(update: Update, context):
    """Mengulang kembali pesan teks yang diterima dari pengguna."""
    await update.message.reply_text(f"Anda bilang: {update.message.text}")

def main():
    """Fungsi utama untuk mengatur dan menjalankan bot."""

    init_google_sheets()

    application = Application.builder().token(TOKEN).build()

    # Conversation Handler untuk menambahkan transaksi
    conv_add_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CHOOSING_MAIN_CATEGORY: [CallbackQueryHandler(choose_main_category)],
            CHOOSING_SUB_CATEGORY: [CallbackQueryHandler(choose_sub_category)],
            ASKING_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_amount)],
            ASKING_POSISI_KAS: [CallbackQueryHandler(ask_posisi_kas)],
            ASKING_KETERANGAN: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_keterangan)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation Handler untuk menghapus transaksi
    conv_delete_handler = ConversationHandler(
        entry_points=[CommandHandler("hapus_transaksi", hapus_transaksi_start)],
        states={
            DELETE_ASKING_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, hapus_transaksi_get_id)],
            DELETE_CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, hapus_transaksi_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation Handler untuk mengedit transaksi
    conv_edit_handler = ConversationHandler(
        entry_points=[CommandHandler("edit_transaksi", edit_transaksi_start)],
        states={
            EDIT_ASKING_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_transaksi_get_id)],
            EDIT_CHOOSING_FIELD: [CallbackQueryHandler(edit_transaksi_choose_field)],
            EDIT_ASKING_NEW_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_transaksi_get_new_value),
                CallbackQueryHandler(edit_transaksi_get_new_value) 
            ],
            EDIT_CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_transaksi_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation Handler untuk mereset semua data
    conv_reset_data_handler = ConversationHandler(
        entry_points=[CommandHandler("reset_data", reset_data_start)],
        states={
            RESET_DATA_CONFIRMATION: [CallbackQueryHandler(reset_data_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_add_handler)
    application.add_handler(conv_delete_handler) 
    application.add_handler(conv_edit_handler) 
    application.add_handler(conv_reset_data_handler) 

    application.add_handler(CommandHandler("ringkasan_hari", ringkasan_hari))
    application.add_handler(CommandHandler("ringkasan_minggu", ringkasan_minggu))
    application.add_handler(CommandHandler("ringkasan_bulan", ringkasan_bulan))
    application.add_handler(CommandHandler("rangkuman_keuangan", rangkuman_keuangan))

    application.add_handler(CommandHandler("export_data", export_data))
    
    application.add_handler(CommandHandler("help", help_command))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    logger.info("Bot sedang berjalan...")
    application.run_polling(poll_interval=3) 

if __name__ == "__main__":
    main()
