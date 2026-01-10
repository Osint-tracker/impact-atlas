import os
import sqlite3
import pandas as pd
import chromadb
from openai import OpenAI
import uuid
from typing import List, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import re

# --- CONFIGURAZIONE PERCORSI (FIX DEFINITIVO) ---
# 1. Percorso assoluto di QUESTO script (refiner.py)
#    Es: C:\...\war_tracker_v2\scripts\refiner.py
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Percorso della root del progetto (war_tracker_v2)
#    Saliamo di un livello (da 'scripts' a 'war_tracker_v2')
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# 3. Costruiamo i percorsi finali verso la cartella 'data'
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "raw_events.db")
CHROMA_PATH = os.path.join(DATA_DIR, "chroma_store")
# Carica il .env dalla root corretta
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# 4. Carichiamo le variabili d'ambiente
#    Specifichiamo il path esatto per evitare errori se lanci da terminali diversi
load_dotenv(dotenv_path=ENV_PATH)

# Debug: Stampiamo per essere sicuri
print(f"[*] Root Progetto: {PROJECT_ROOT}")
print(f"[*] DB Path atteso: {DB_PATH}")

# Controllo esistenza cartella 'data'
if not os.path.exists(DATA_DIR):
    print(f"[*] ATTENZIONE: La cartella {DATA_DIR} non esisteva. La creo.")
    os.makedirs(DATA_DIR)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("❌ ERRORE CRITICO: OPENAI_API_KEY non trovata. Controlla il file .env!")

# --- PARAMETRI DI ELABORAZIONE ---
EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 75
SIMILARITY_THRESHOLD = 0.22
LOOKBACK_WINDOW_DAYS = 30
STRICT_WINDOW_HOURS = 36

# --- FILTRI DI RILEVANZA (WHITELIST ESTESA) ---
# Uniamo più liste per mantenere il codice leggibile.
# Include termini in Inglese, Italiano e traslitterazioni comuni (es. Kyiv/Kiev).

# ==============================================================================
# SEZIONE 1: GDELT OPTIMIZED LISTS (ENGLISH ONLY)
# Utilizzare queste liste per query su database globali (es. GDELT API)
# dove il contenuto è già tradotto o indicizzato in inglese.
# ==============================================================================

GEO_LOCATIONS_EN = [
    "Ukraine", "Kyiv", "Kiev", "Donbas", "Donetsk", "Luhansk", "Kharkiv",
    "Kherson", "Zaporizhzhia", "Crimea", "Sevastopol", "Mariupol", "Bakhmut",
    "Avdiivka", "Chasiv Yar", "Vovchansk", "Kupiansk", "Lyman", "Robotyne",
    "Sumy", "Chernihiv", "Odesa", "Odessa", "Lviv", "Dnipro", "Mykolaiv",
    "Poltava", "Vinnytsia", "Zhytomyr", "Kryvyi Rih", "Kremenchuk",
    "Snake Island", "Black Sea", "Sea of Azov", "Kerch", "Belgorod",
    "Kursk", "Voronezh", "Rostov", "Bryansk", "Transnistria"
]

WEAPONS_AND_TECH_EN = [
    "HIMARS", "ATACMS", "Patriot missile", "NASAMS", "IRIS-T", "S-300", "S-400",
    "S-500", "Kinzhal", "Kalibr", "Iskander", "Zircon missile", "Kh-101", "Kh-55",
    "Kh-59", "Shahed drone", "Geran drone", "Lancet drone", "FPV drone", "UAV",
    "Bayraktar", "Leopard tank", "Abrams tank", "Challenger tank", "Bradley",
    "Marder", "Stryker", "CV90", "T-72", "T-80", "T-90", "BMP", "BTR",
    "Artillery", "Howitzer", "Caesar system", "Archer system", "Krab", "PzH-2000",
    "Cluster munition", "Storm Shadow", "SCALP missile", "Taurus missile", "F-16",
    "MiG-29", "Su-24", "Su-25", "Su-27", "Su-34", "Su-35", "Su-57", "Tu-95",
    "Tu-22", "Tu-160", "A-50", "Black Sea Fleet", "Pantsir", "Loitering munition",
    "Kamikaze drone", "Javelin", "Stinger", "NLAW",
    "Flamingo drone", "FP-1 drone", "FP-2 drone", "FP-5", "FP-7", "FP-9"
]

KEY_ACTORS_EN = [
    "Zelensky", "Zelenskyy", "Putin", "Biden", "Trump", "Scholz", "Macron", "Merz",
    "Sunak", "Starmer", "Von der Leyen", "Stoltenberg", "Rutte", "Syrsky", "Syrskyi",
    "Zaluzhny", "Budanov", "Shoigu", "Gerasimov", "Belousov", "Prigozhin", "Wagner Group",
    "Kadyrov", "Akhmat", "Azov Regiment", "Kraken Regiment", "SBU", "GUR", "FSB", "GRU",
    "Russian Ministry of Defense", "Armed Forces of Ukraine"
]

KEYWORDS_GENERAL_EN = [
    "Russia", "Moscow", "Kremlin", "NATO", "EU", "European Union",
    "War", "Invasion", "Conflict", "Missile strike", "Rocket", "Shelling",
    "Bombardment", "Explosion", "Blast", "Air raid alert", "Air defense",
    "Frontline", "Trench warfare", "Offensive", "Counteroffensive",
    "Mobilization", "Sanctions", "Aid package", "Grain corridor",
    "POW", "Prisoners of war", "Prisoner exchange"
]

# ==============================================================================
# SEZIONE 2: FULL MATCHING LISTS (ORIGINAL + RUSSIAN + UKRAINIAN)
# Utilizzare queste liste per Regex su testo grezzo (Raw Text), scraping,
# canali Telegram o fonti locali non tradotte.
# ==============================================================================

# --- A. LISTE ORIGINALI (BASE) ---
GEO_LOCATIONS_BASE = [
    "ukrain", "ucrain", "kiev", "kyiv", "donbas", "donetsk", "luhansk", "lugansk",
    "kharkiv", "kharkov", "kherson", "zaporizh", "zaporozhye", "crimea", "sevastopol",
    "mariupol", "bakhmut", "artemovsk", "avdiivka", "chasiv yar", "vovchansk",
    "kupiansk", "lyman", "robotyne", "sumy", "chernihiv", "odessa", "odesa",
    "lviv", "leopoli", "dnipro", "mykolaiv", "poltava", "vinnytsia", "zhytomyr",
    "kryvyi rih", "kremenchuk", "snake island", "black sea", "mar nero", "azov",
    "kerch", "belgorod", "kursk", "voronezh", "rostov", "bryansk", "transnistria"
]

WEAPONS_AND_TECH_BASE = [
    "himars", "atacms", "patriot", "nasams", "iris-t", "s-300", "s-400", "s-500",
    "kinzhal", "kalibr", "iskander", "zircon", "tsirkon", "kh-101", "kh-55", "kh-59",
    "shahed", "geran", "lancet", "fpv", "drone", "uav", "bayraktar", "tb2",
    "leopard", "abrams", "challenger", "bradley", "marder", "stryker", "cv90",
    "t-72", "t-80", "t-90", "bmp", "btr", "artillery", "howitzer", "caesar",
    "archer", "krab", "phz-2000", "cluster munition", "storm shadow", "scalp",
    "taurus", "f-16", "f16", "mig-29", "su-24", "su-25", "su-27", "su-34", "su-35",
    "su-57", "tu-95", "tu-22", "tu-160", "a-50", "black fleet", "pantsir", "FPV",
    "loitering munition", "drone kamikaze", "missile", "Missiles", "javelin",
    "stinger", "nlaw", "FP-1", "FP-2", "FP-5", "Flamingo", "FP-7", "FP-9"
]

KEY_ACTORS_BASE = [
    "zelensky", "zelenskyy", "putin", "biden", "trump", "scholtz", "macron", "merz",
    "sunak", "starmer", "von der leyen", "stoltenberg", "rutte", "syrsky", "syrskyi",
    "zaluzhny", "budanov", "shoigu", "gerasimov", "belousov", "prigozhin", "wagner",
    "kadyrov", "akhmat", "azov", "kraken", "sbu", "gur", "fsb", "gru", "mod russia",
    "zsu", "afu", "armed forces", "forze armate", "ministero della difesa", "defense ministry"
]

KEYWORDS_GENERAL_BASE = [
    "russia", "moscow", "mosca", "kremlin", "cremlino", "nato", "eu", "ue",
    "war", "guerra", "invasion", "invasi", "conflict", "conflitto",
    "missil", "rocket", "shelling", "bombard", "explosion", "esplosion", "blast",
    "air raid", "allarme aereo", "air defense", "difesa aerea", "contraerea",
    "frontline", "fronte", "trench", "trincea", "offensive", "offensiva",
    "counteroffensive", "controffensiva", "mobilization", "mobilitazione",
    "sanctions", "sanzioni", "aid package", "pacchetto aiuti", "grain corridor",
    "pow", "prisoners", "prigionieri", "exchange", "scambio"
]

# --- B. TRADUZIONI (RUSSO E UCRAINO) ---
GEO_LOCATIONS_TRANS = [
    "Украина", "Україна", "Киев", "Київ", "Донбасс", "Донбас", "Донецк", "Донецьк",
    "Луганск", "Луганськ", "Харьков", "Харків", "Херсон", "Херсон", "Запорожье", "Запоріжжя",
    "Крым", "Крим", "Севастополь", "Севастополь", "Мариуполь", "Маріуполь", "Бахмут", "Бахмут",
    "Артемовск", "Авдеевка", "Авдіївка", "Часов Яр", "Часів Яр", "Волчанск", "Вовчанськ",
    "Купянск", "Куп'янськ", "Лиман", "Лиман", "Работино", "Роботине", "Сумы", "Суми",
    "Чернигов", "Чернігів", "Одесса", "Одеса", "Львов", "Львів", "Днепр", "Дніпро",
    "Николаев", "Миколаїв", "Полтава", "Полтава", "Винница", "Вінниця", "Житомир", "Житомир",
    "Кривой Рог", "Кривий Ріг", "Кременчуг", "Кременчук", "Остров Змеиный", "Острів Зміїний",
    "Черное море", "Чорне море", "Азовское море", "Азовське море", "Керчь", "Керч",
    "Белгород", "Бєлгород", "Курск", "Курськ", "Воронеж", "Воронеж", "Ростов", "Ростов",
    "Брянск", "Брянськ", "Приднестровье", "Придністров'я"
]

WEAPONS_AND_TECH_TRANS = [
    "Хаймарс", "HIMARS", "ATACMS", "АТАКМС", "Пэтриот", "Петріот", "Насамс", "NASAMS",
    "Ирис-Т", "Iris-T", "С-300", "С-400", "С-500", "Кинжал", "Кинджал", "Калибр", "Калібр",
    "Искандер", "Іскандер", "Циркон", "Циркон", "Х-101", "Х-55", "Х-59", "Шахед", "Шахед",
    "Герань", "Герань", "Ланцет", "Ланцет", "ФПВ", "FPV", "Дрон", "БПЛА", "Безпілотник",
    "Байрактар", "Байрактар", "Леопард", "Леопард", "Абрамс", "Абрамс", "Челенджер",
    "Челенджер", "Брэдли", "Бредлі", "Мардер", "Мардер", "Страйкер", "Страйкер",
    "CV90", "СV90", "Т-72", "Т-80", "Т-90", "БМП", "БТР", "Артиллерия", "Артилерія",
    "Гаубица", "Гаубиця", "Цезарь", "Цезар", "Арчер", "Archer", "Краб", "Krab", "PzH 2000",
    "Кассетные боеприпасы", "Касетні боєприпаси", "Шторм Шэдоу", "Storm Shadow",
    "Скалп", "Scalp", "Таурус", "Taurus", "Ф-16", "F-16", "МиГ-29", "МіГ-29",
    "Су-24", "Су-25", "Су-27", "Су-34", "Су-35", "Су-57", "Ту-95", "Ту-22", "Ту-160",
    "А-50", "Черноморский флот", "Чорноморський флот", "Панцирь", "Панцир",
    "Барражирующий боеприпас", "Баражуючий боєприпас", "Дрон-камикадзе", "Дрон-камікадзе",
    "Ракета", "Ракети", "Джавелин", "Джавелін", "Стингер", "Стінгер", "NLAW",
    "ФП-1", "FP-1", "ФП-2", "FP-2", "ФП-5", "FP-5", "ФП-7", "FP-7", "ФП-9", "FP-9",
    "Фламинго", "Фламінго"
]

KEY_ACTORS_TRANS = [
    "Зеленский", "Зеленський", "Путин", "Путін", "Байден", "Байден", "Трамп", "Трамп",
    "Шольц", "Шольц", "Макрон", "Макрон", "Мерц", "Мерц", "Сунак", "Сунак",
    "Стармер", "Стармер", "Фон дер Ляйен", "Фон дер Ляєн", "Столтенберг", "Столтенберг",
    "Рютте", "Рютте", "Сырский", "Сирський", "Залужный", "Залужний", "Буданов", "Буданов",
    "Шойгу", "Шойгу", "Герасимов", "Герасимов", "Белоусов", "Бєлоусов", "Пригожин", "Пригожин",
    "Вагнер", "Вагнер", "ЧВК Вагнер", "Кадыров", "Кадиров", "Ахмат", "Ахмат", "Азов", "Азов",
    "Кракен", "Кракен", "СБУ", "СБУ", "ГУР", "ГУР", "ФСБ", "ФСБ", "ГРУ", "ГРУ",
    "Минобороны РФ", "Міноборони РФ", "ВСУ", "ЗСУ", "Збройні Сили",
    "Министерство обороны", "Міністерство оборони"
]

KEYWORDS_GENERAL_TRANS = [
    "Россия", "Росія", "Москва", "Москва", "Кремль", "Кремль", "НАТО", "НАТО",
    "ЕС", "ЄС", "Євросоюз", "Война", "Війна", "Вторжение", "Вторгнення",
    "Конфликт", "Конфлікт", "Ракетный удар", "Ракетний удар", "Обстрел", "Обстріл",
    "Бомбардировка", "Бомбардування", "Взрыв", "Вибух",
    "Воздушная тревога", "Повітряна тривога", "ПВО", "ППО", "Линия фронта", "Лінія фронту",
    "Окоп", "Траншея", "Наступление", "Наступ", "Контрнаступление", "Контрнаступ",
    "Мобилизация", "Мобілізація", "Санкции", "Санкції", "Пакет помощи", "Пакет допомоги",
    "Зерновой коридор", "Зерновий коридор", "Военнопленные", "Військовополонені",
    "Обмен пленными", "Обмін полоненими"
]

# ==============================================================================
# SEZIONE 3: LISTA AGGREGATA FINALE
# ==============================================================================
RELEVANCE_KEYWORDS = GEO_LOCATIONS_BASE + GEO_LOCATIONS_TRANS + \
    WEAPONS_AND_TECH_BASE + WEAPONS_AND_TECH_TRANS + \
    KEY_ACTORS_BASE + KEY_ACTORS_TRANS + \
    KEYWORDS_GENERAL_BASE + KEYWORDS_GENERAL_TRANS

# ==============================================================================
# SEZIONE 4: FILTRO ANCORA (CRITICO PER EVITARE GAZA/USA)
# ==============================================================================
# Queste parole DEVONO esserci. Se un articolo parla di "Biden" o "Missili"
# ma non cita nessuna di queste (es. non cita Russia o Ucraina), viene scartato.
# ==============================================================================

# Uniamo solo Luoghi Geografici e Attori Diretti (Putin/Zelensky/Generali)
# Escludiamo: Biden, NATO, UE, Missili, Guerra (perché troppo generici)
MANDATORY_ROOTS = GEO_LOCATIONS_BASE + GEO_LOCATIONS_TRANS + [
    "zelensky", "zelenskyy", "зеленский", "зеленський",
    "putin", "путин", "путін",
    "shoigu", "шойгу", "gerasimov", "герасимов",
    "syrsky", "syrskyi", "сирський", "сырский",
    "zaluzhny", "залужный", "залужний",
    "budanov", "буданов",
    "russia", "russian", "moscow", "kremlin",
    "россия", "росія", "москва", "кремль",
    "ukraine", "ukrainian", "kyiv", "kiev",
    "украина", "україна", "киев", "київ"
]

# --- GAZETTEER EXTERNAL LOADER (Miglioramento IA) ---
GAZETTEER_PATH = os.path.join(DATA_DIR, "gazetteer_places.txt")
if os.path.exists(GAZETTEER_PATH):
    try:
        with open(GAZETTEER_PATH, 'r', encoding='utf-8') as f:
            places = [line.strip().lower() for line in f if line.strip()]
            print(f"[*] GAZETTEER ATTIVO: Caricati {len(places)} luoghi extra (High Precision Mode).")
            # Aggiungiamo alla lista Mandatory
            MANDATORY_ROOTS.extend(places)
    except Exception as e:
        print(f"[!] Errore caricamento Gazetteer: {e}")

# Ottimizzazione: Set per ricerca istantanea
MANDATORY_SET = list(set(k.lower() for k in MANDATORY_ROOTS if k))
print(
    f"[*] Filtro 'Ancora' attivo: {len(MANDATORY_SET)} termini obbligatori (Russia/Ucraina/Città).")

# --- OTTIMIZZAZIONE CRITICA ---
# Convertiamo tutto in minuscolo ORA e rimuoviamo duplicati.
# Usiamo questo set per il controllo veloce.
RELEVANCE_KEYWORDS_LOWER = list(set(k.lower()
                                for k in RELEVANCE_KEYWORDS if k))
print(
    f"[*] Keywords caricate e normalizzate: {len(RELEVANCE_KEYWORDS_LOWER)} termini.")


class WarRefiner:
    def __init__(self):
        print(f"[*] Inizializzazione Refiner (Mode: HYBRID TEXT VERIFICATION)...")

        # 1. Setup SQLite
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

        # 2. Setup OpenAI Client
        self.openai_client = OpenAI(api_key=OPENAI_API_KEY)

        # 3. Setup ChromaDB
        self.chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

        # Setup Collezione con distanza Coseno
        self.collection = self.chroma_client.get_or_create_collection(
            name="war_events_v2",
            metadata={"hnsw:space": "cosine"}
        )
        print(
            f"[*] ChromaDB connesso. Lookback: {LOOKBACK_WINDOW_DAYS} giorni. Strict Window: {STRICT_WINDOW_HOURS}h")

    def parse_date_to_timestamp(self, date_str: str) -> float:
        """Converte YYYYMMDDHHMMSS in Unix Timestamp float."""
        try:
            d_str = str(date_str).ljust(14, '0')[:14]
            dt = datetime.strptime(d_str, "%Y%m%d%H%M%S")
            return dt.timestamp()
        except ValueError:
            return datetime.now().timestamp()

    def verify_date_match(self, text_content: str, target_ts: float) -> bool:
        """
        Setaccio Fine: Cerca nel testo se viene menzionata la data del target.
        Utile per articoli pubblicati giorni dopo l'evento.
        """
        target_dt = datetime.fromtimestamp(target_ts)
        text_lower = text_content.lower()

        # 1. Cerca formato ISO (es. 2023-05-12)
        iso_date = target_dt.strftime("%Y-%m-%d")
        if iso_date in text_lower:
            return True

        # 2. Cerca formato Giorno/Mese (es. 12/05)
        slash_date = target_dt.strftime("%d/%m")
        if slash_date in text_lower:
            return True

        # 3. Cerca Giorno + Nome Mese (es. "12 maggio", "12 may")
        day = str(target_dt.day)
        # Mappatura minima mesi (espandibile)
        months_en = ["jan", "feb", "mar", "apr", "may",
                     "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
        months_it = ["gen", "feb", "mar", "apr", "mag",
                     "giu", "lug", "ago", "set", "ott", "nov", "dic"]

        month_idx = target_dt.month - 1

        # Controlliamo pattern semplici "12 may" o "12 maggio"
        patterns = [
            f"{day} {months_en[month_idx]}",
            f"{day} {months_it[month_idx]}"
        ]

        for p in patterns:
            if p in text_lower:
                return True

        return False

    def fetch_unprocessed_batch(self, limit=100) -> pd.DataFrame:
        query = """
        SELECT event_hash, date_published, source_type, source_name, text_content, url
        FROM raw_signals
        WHERE is_embedded = 0
        LIMIT ?
        """
        return pd.read_sql_query(query, self.conn, params=(limit,))

    @retry(wait=wait_exponential(multiplier=1, min=2, max=20), stop=stop_after_attempt(5))
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        cleaned_texts = [str(t)[:6000] for t in texts]
        response = self.openai_client.embeddings.create(
            input=cleaned_texts,
            model=EMBEDDING_MODEL
        )
        return [data.embedding for data in response.data]

    def process_clustering_and_storage(self, df: pd.DataFrame, embeddings: List[List[float]]):
        ids_to_upsert = []
        embeddings_to_upsert = []
        metadatas_to_upsert = []
        documents_to_upsert = []
        updates_for_sqlite = []

        # --- FIX CRITICO: USIAMO ZIP ---
        # Iteriamo simultaneamente sulle righe del DataFrame filtrato e sulla lista degli embeddings.
        # Questo evita l'IndexError causato dagli indici originali del DataFrame.
        for (_, row), current_embedding in zip(df.iterrows(), embeddings):

            event_hash = row['event_hash']

            # Timestamp dell'articolo CORRENTE
            pub_ts = self.parse_date_to_timestamp(row['date_published'])

            # --- LOGICA TEMPORALE IBRIDA ---
            start_window = pub_ts - (LOOKBACK_WINDOW_DAYS * 24 * 3600)
            end_window = pub_ts + (24 * 3600)

            cluster_id = None
            is_new_cluster = True

            # Query Chroma
            if self.collection.count() > 0:
                query_results = self.collection.query(
                    query_embeddings=[current_embedding],
                    n_results=1,
                    where={
                        "$and": [
                            {"timestamp": {"$gte": start_window}},
                            {"timestamp": {"$lte": end_window}}
                        ]
                    },
                    include=["metadatas", "distances"]
                )

                if query_results['ids'] and query_results['distances'][0]:
                    existing_distance = query_results['distances'][0][0]
                    existing_metadata = query_results['metadatas'][0][0]
                    existing_ts = existing_metadata.get('timestamp', 0)

                    # 1. Check Semantico (Soglia Dinamica)
                    # Testi brevi (< 400 char) sono spesso ambigui/generici -> Richiedono similarità più alta
                    # Testi lunghi possono variare di più -> Soglia standard
                    text_len = len(str(row['text_content']))
                    dynamic_threshold = SIMILARITY_THRESHOLD
                    
                    if text_len < 400:
                        dynamic_threshold = 0.15  # Molto severo per one-liners (richiede distanza < 0.15)
                    elif text_len < 1000:
                        dynamic_threshold = 0.20  # Severo medio
                        
                    # Nota: Distanza Coseno Minore = Più Simile
                    if existing_distance < dynamic_threshold:

                        time_diff_hours = abs(pub_ts - existing_ts) / 3600
                        match_confirmed = False

                        # 2A. Check Temporale STRETTO
                        if time_diff_hours <= STRICT_WINDOW_HOURS:
                            match_confirmed = True

                        # 2B. Check Temporale LARGO + Verifica Testo
                        else:
                            if self.verify_date_match(row['text_content'], existing_ts):
                                match_confirmed = True

                        if match_confirmed:
                            cluster_id = existing_metadata.get('cluster_id')
                            is_new_cluster = False

            # Assegnazione Cluster ID
            if is_new_cluster:
                cluster_id = str(uuid.uuid4())

            # Preparazione Dati
            ids_to_upsert.append(event_hash)
            embeddings_to_upsert.append(current_embedding)
            # Tagliamo il testo per evitare errori di dimensione massima metadata/document
            documents_to_upsert.append(str(row['text_content'])[:1000])

            metadatas_to_upsert.append({
                "source": str(row['source_name']),
                "type": str(row['source_type']),
                "date_str": str(row['date_published']),
                "timestamp": pub_ts,
                "cluster_id": cluster_id,
                "url": str(row['url'])
            })

            updates_for_sqlite.append((cluster_id, event_hash))

        # SCRITTURA BATCH SU CHROMA
        if ids_to_upsert:
            self.collection.upsert(
                ids=ids_to_upsert,
                embeddings=embeddings_to_upsert,
                metadatas=metadatas_to_upsert,
                documents=documents_to_upsert
            )

        # SCRITTURA BATCH SU SQLITE
        if updates_for_sqlite:
            self.cursor.executemany("""
                UPDATE raw_signals 
                SET is_embedded = 1, cluster_id = ? 
                WHERE event_hash = ?
            """, updates_for_sqlite)
            self.conn.commit()

        return len(ids_to_upsert)

    def filter_batch_relevance(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        relevant_indices = []

        # DEBUG SAMPLE
        if not df.empty:
            print(f"\n--- [CHECK BATCH INPUT] ---")
            print(f"URL Sample: {df.iloc[0]['url']}")
            print(f"---------------------------\n")

        for idx, row in df.iterrows():
            text = row['text_content']
            url = row['url']

            # Preparazione contenuto (Testo + URL)
            content_to_check = ""
            if isinstance(text, str):
                content_to_check += text.lower()
            if isinstance(url, str):
                content_to_check += " " + url.lower()

            if not content_to_check.strip():
                continue

            # --- IL NUOVO FILTRO A DUE LIVELLI ---

            # 1. CONTROLLO OBBLIGATORIO (L'Ancora)
            # Deve contenere ALMENO una parola chiave "forte" (Ucraina, Russia, Kiev, ecc.)
            has_mandatory_context = False
            for anchor in MANDATORY_SET:
                if anchor in content_to_check:
                    has_mandatory_context = True
                    break

            # Se non parla esplicitamente di Russia/Ucraina/Luoghi, SCARTA SUBITO.
            # Questo elimina "Biden parla di tasse" o "Guerra a Gaza".
            if not has_mandatory_context:
                continue

            # 2. CONTROLLO DI RILEVANZA GENERALE (Opzionale ma utile)
            # Se ha superato il controllo 1, è quasi sicuramente buono,
            # ma facciamo un check se contiene una qualsiasi delle nostre keyword
            # (che include anche armi, droni, ecc.)
            is_match = False
            for keyword in RELEVANCE_KEYWORDS_LOWER:
                if keyword in content_to_check:
                    is_match = True
                    break

            if is_match:
                relevant_indices.append(idx)

        relevant_df = df.loc[relevant_indices].copy()
        skipped_df = df.drop(relevant_indices).copy()

        print(
            f"   -> Rilevanti (con contesto): {len(relevant_df)} | Scartati: {len(skipped_df)}")
        return relevant_df, skipped_df

    def mark_skipped_rows(self, df_skipped: pd.DataFrame):
        """Imposta is_embedded = 2 (IGNORED) per le righe irrilevanti."""
        if df_skipped.empty:
            return

        skipped_ids = [(row['event_hash'],)
                       for _, row in df_skipped.iterrows()]

        # Aggiorniamo SQLite per non ripescarli più
        self.cursor.executemany("""
            UPDATE raw_signals 
            SET is_embedded = 2 
            WHERE event_hash = ?
        """, skipped_ids)
        self.conn.commit()
        print(
            f"   [SKIP] Ignorati {len(skipped_ids)} eventi non pertinenti (Gaza/Taiwan/Altro).")

    def run(self):
        print(
            f"Starting Refinery Pipeline (Batch: {BATCH_SIZE}, Mode: SMART FILTER)...")
        total_processed = 0
        total_skipped = 0

        try:
            while True:
                # 1. Fetch dal DB
                df = self.fetch_unprocessed_batch(BATCH_SIZE)
                if df.empty:
                    print("Tutti gli eventi sono stati processati. Standby.")
                    break

                # 2. --- FILTRO INTELLIGENTE ---
                # Dividiamo il grano dalla pula
                df_relevant, df_skipped = self.filter_batch_relevance(df)

                # Segniamo subito quelli scartati nel DB (is_embedded = 2)
                if not df_skipped.empty:
                    self.mark_skipped_rows(df_skipped)
                    total_skipped += len(df_skipped)

                # Se non è rimasto nulla di rilevante in questo batch, passiamo al prossimo
                if df_relevant.empty:
                    print("   [INFO] Batch interamente scartato. Continuo...")
                    continue

                # 3. Embeddings (SOLO sui rilevanti -> RISPARMIO $$$)
                try:
                    # Ricordati di usare df_relevant['text_content']
                    embeddings = self.generate_embeddings_batch(
                        df_relevant['text_content'].tolist())
                except Exception as e:
                    print(f"Error generating embeddings: {e}")
                    time.sleep(10)
                    continue

                # 4. Cluster & Store
                count = self.process_clustering_and_storage(
                    df_relevant, embeddings)
                total_processed += count

                print(
                    f"Batch completato. Processati: {count} | Ignorati: {len(df_skipped)} | Totale Sessione: {total_processed}")

        except KeyboardInterrupt:
            print("\nChiusura sicura richiesta dall'utente...")
        finally:
            self.conn.close()
            print("Connessione DB chiusa.")


if __name__ == "__main__":
    refiner = WarRefiner()
    refiner.run()
