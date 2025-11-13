import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os
import random
from pathlib import Path
from datetime import datetime
import hashlib

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False
MAX_REINTENTOS = 3  # Número máximo de reintentos por URL

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
    # --- TikTok ---
    "https://vt.tiktok.com/ZSynjCxPs/",
    "https://vt.tiktok.com/ZSynjXVkv/",
    "https://vt.tiktok.com/ZSynj9uqF/",
    "https://vt.tiktok.com/ZSynjbfUp/",
    "https://vt.tiktok.com/ZSynjHRu3/",
    "https://vt.tiktok.com/ZSynjktgn/",
    "https://vt.tiktok.com/ZSynj9YYK/",
    "https://vt.tiktok.com/ZSynjAG7C/",
    "https://vt.tiktok.com/ZSy8ENMRW/",

    # --- Facebook ---
    "https://www.facebook.com/100064867445065/posts/1260808236091413/",
    "https://www.facebook.com/100064867445065/posts/1260808222758081/",
    "https://www.facebook.com/100064867445065/posts/1260808139424756/",
    "https://www.facebook.com/100064867445065/posts/1260822012756702/",
    "https://www.facebook.com/100064867445065/posts/1261357606036476/",
    "https://www.facebook.com/100064867445065/posts/1261357616036475/",
    "https://www.facebook.com/100064867445065/posts/pfbid023bwGRUTbkVJCm1VteAwQ4o8Z9qw6zAXw4B1XohbBQgxs1q2wygScrorq8oTK7ccYl",
    "https://www.facebook.com/100064867445065/posts/pfbid029hT2ZWY4t8TUmLiLrJGj4Ris4NppCrZpRrzjEigT59GB1QDoFubpUtCD9nuevefWl",
    "https://www.facebook.com/100064867445065/posts/pfbid02BWQCsWUatN4ELYBsTsiGH58AsMR2R7guz8LGT3PHUmpXE2CVtis4pS1FYv6QmaQul",
    "https://www.facebook.com/100064867445065/posts/pfbid02PhkXpJn3DZ7gNSrftGSFV5KygNETAVZsrkikNhj1MyudCJAUvHaraAfN7igoh2nvl",
    "https://www.facebook.com/100064867445065/posts/pfbid02XVfbLpzFXhmK2VTzR5GrmPAP75SERcqSHTdXidveFDKpwqbYcnt8e2DdTbHK5Srul",
    "https://www.facebook.com/100064867445065/posts/pfbid02ycxCqjBJ6hEWoUQ4eRDjWPz28VVYpdfGsQ1aYmLcFuov7kYMFqoMCEDuqfMLVy2Sl",
    "https://www.facebook.com/100064867445065/posts/pfbid0J6JtaE3Wz43drqg4gG7thMV8QeAa3eKmDuQML3RjU1aYWXaTp6gjy8aTwoasLf35l",
    "https://www.facebook.com/100064867445065/posts/pfbid0MeAdgNbtFKofjnq5RvQ68RcSyXXbRFRTGQ4p8GaBKrnDKtCanp82NqNz8tEhZRoGl",
    "https://www.facebook.com/100064867445065/posts/pfbid0iWSJqTLCzeTBgz7LSHbiDGqYbxC7cbhgfHGDVEt1r5CB8AwgjuV9xxtEwvQaZEGnl",
    "https://www.facebook.com/reel/848611304195556/",
    "https://www.facebook.com/reel/1135439792045406/",

    # --- Instagram ---
    "https://www.instagram.com/p/DQHeaGojC-y/",
    "https://www.instagram.com/p/DQHedmRjHLo/",
    "https://www.instagram.com/p/DQHeZ1LjOge/",
    "https://www.instagram.com/p/DQXX191jDIm/",
    "https://www.instagram.com/p/DQXXzQWDDZV/",
    "https://www.instagram.com/p/DQXX1g-jBg9/",
    "https://www.instagram.com/p/DQXaV3BjM4E/",
    "https://www.instagram.com/p/DQMhtk1DGri/",
    "https://www.instagram.com/p/DQ9jdxAjP_x/",
    "https://www.instagram.com/p/DQ9jtyTjD2N/",
    "https://www.instagram.com/p/DQ9jvTNDGNb/",
]

# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)
        self.failed_urls = []  # Track URLs that failed after all retries

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com', 'fb.me']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url or 'vt.tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_with_retry(self, scrape_function, url, max_comments, campaign_info, post_number):
        """Ejecuta una función de scraping con reintentos"""
        for attempt in range(MAX_REINTENTOS):
            try:
                result = scrape_function(url, max_comments, campaign_info, post_number)
                if result:  # Si obtuvimos resultados, retornar
                    return result
                if attempt < MAX_REINTENTOS - 1:
                    wait_time = (attempt + 1) * 30  # Espera incremental
                    logger.warning(f"Attempt {attempt + 1} failed. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < MAX_REINTENTOS - 1:
                    wait_time = (attempt + 1) * 30
                    time.sleep(wait_time)
        
        # Si llegamos aquí, todos los intentos fallaron
        self.failed_urls.append(url)
        logger.error(f"All attempts failed for URL: {url}")
        return []

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Error in scrape_facebook_comments: {e}")
            raise  # Re-raise para que el retry lo maneje

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Error in scrape_instagram_comments: {e}")
            raise

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Error in scrape_tiktok_comments: {e}")
            raise

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {
                **campaign_info,
                'post_url': url,  # Mantener URL original para preservar unicidad
                'post_url_original': url,
                'post_number': post_number,
                'platform': 'Facebook',
                'author_name': self.fix_encoding(comment.get('authorName')),
                'author_url': comment.get('authorUrl'),
                'comment_text': self.fix_encoding(comment.get('text')),
                'created_time': created_time,
                'likes_count': comment.get('likesCount', 0),
                'replies_count': comment.get('repliesCount', 0),
                'is_reply': False,
                'parent_comment_id': None,
                'created_time_raw': str(comment)[:500]  # Limitar longitud
            }
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {
                    **campaign_info,
                    'post_url': url,  # Mantener URL original
                    'post_url_original': url,
                    'post_number': post_number,
                    'platform': 'Instagram',
                    'author_name': self.fix_encoding(author),
                    'author_url': f"https://instagram.com/{author}",
                    'comment_text': self.fix_encoding(comment.get('text')),
                    'created_time': created_time,
                    'likes_count': comment.get('likesCount', 0),
                    'replies_count': 0,
                    'is_reply': False,
                    'parent_comment_id': None,
                    'created_time_raw': str(comment)[:500]
                }
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {
                **campaign_info,
                'post_url': url,  # Mantener URL original
                'post_url_original': url,
                'post_number': post_number,
                'platform': 'TikTok',
                'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')),
                'author_url': f"https://www.tiktok.com/@{author_id}",
                'comment_text': self.fix_encoding(comment.get('text')),
                'created_time': comment.get('createTime'),
                'likes_count': comment.get('diggCount', 0),
                'replies_count': comment.get('replyCommentTotal', 0),
                'is_reply': 'replyToId' in comment,
                'parent_comment_id': comment.get('replyToId'),
                'created_time_raw': str(comment)[:500]
            }
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed


def create_post_registry_entry(url, platform, campaign_info, post_number):
    """Crea una entrada de registro para una pauta procesada sin comentarios"""
    return {
        **campaign_info,
        'post_url': url,  # Mantener URL original
        'post_url_original': url,
        'post_number': post_number,  # Ahora siempre incluye post_number
        'platform': platform,
        'author_name': None,
        'author_url': None,
        'comment_text': None,
        'created_time': None,
        'likes_count': 0,
        'replies_count': 0,
        'is_reply': False,
        'parent_comment_id': None,
        'created_time_raw': None,
        'extraction_status': 'NO_COMMENTS'  # Nuevo campo para tracking
    }


def create_failed_registry_entry(url, platform, campaign_info, post_number):
    """Crea una entrada de registro para una URL que falló la extracción"""
    return {
        **campaign_info,
        'post_url': url,
        'post_url_original': url,
        'post_number': post_number,
        'platform': platform,
        'author_name': None,
        'author_url': None,
        'comment_text': None,
        'created_time': None,
        'likes_count': 0,
        'replies_count': 0,
        'is_reply': False,
        'parent_comment_id': None,
        'created_time_raw': None,
        'extraction_status': 'FAILED'  # Indicar que la extracción falló
    }


def create_unique_comment_hash(row):
    """Crea un hash único para cada comentario basado en múltiples campos"""
    if pd.isna(row.get('comment_text')) or str(row.get('comment_text', '')).strip() == '':
        # Es una entrada de registro
        return f"REGISTRY_{row.get('post_url', '')}_{row.get('extraction_status', 'UNKNOWN')}"
    
    # Crear un string único basado en múltiples campos
    unique_string = f"{row.get('platform', '')}|{row.get('post_url', '')}|{row.get('author_name', '')}|{row.get('comment_text', '')}|{row.get('created_time', '')}"
    
    # Generar hash MD5 para evitar IDs muy largos
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()


def merge_comments(df_existing, df_new):
    """Combina comentarios existentes con nuevos, evitando duplicados reales"""
    if df_existing.empty:
        return df_new
    if df_new.empty:
        return df_existing
    
    logger.info(f"Merging: {len(df_existing)} existing + {len(df_new)} new rows")
    
    # Crear hashes únicos para identificación
    df_existing['_comment_hash'] = df_existing.apply(create_unique_comment_hash, axis=1)
    df_new['_comment_hash'] = df_new.apply(create_unique_comment_hash, axis=1)
    
    # Encontrar comentarios verdaderamente nuevos
    existing_hashes = set(df_existing['_comment_hash'])
    df_truly_new = df_new[~df_new['_comment_hash'].isin(existing_hashes)].copy()
    
    logger.info(f"Found {len(df_truly_new)} truly new entries")
    
    # Para las URLs que tienen nuevos comentarios, actualizar el extraction_status
    urls_with_new_comments = set(df_truly_new[df_truly_new['comment_text'].notna()]['post_url'].unique())
    
    if urls_with_new_comments:
        # Marcar las entradas de registro antiguas para estas URLs
        mask_to_remove = (
            df_existing['comment_text'].isna() & 
            df_existing['post_url'].isin(urls_with_new_comments) &
            (df_existing.get('extraction_status', '') == 'NO_COMMENTS')
        )
        df_existing = df_existing[~mask_to_remove].copy()
        logger.info(f"Removed {mask_to_remove.sum()} obsolete registry entries")
    
    # Combinar dataframes
    df_combined = pd.concat([df_existing, df_truly_new], ignore_index=True)
    df_combined = df_combined.drop(columns=['_comment_hash'])
    
    return df_combined


def save_to_excel(df, filename, scraper=None):
    """Guarda el DataFrame en Excel con múltiples hojas de resumen"""
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Hoja principal de comentarios
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            
            # Resumen por posts
            if not df.empty and 'post_number' in df.columns:
                # Resumen general
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(
                    Total_Comentarios=('comment_text', lambda x: x.notna().sum()),
                    Total_Likes=('likes_count', 'sum'),
                    Primera_Extraccion=('created_time_processed', lambda x: x.min() if x.notna().any() else None),
                    Ultima_Extraccion=('created_time_processed', lambda x: x.max() if x.notna().any() else None)
                ).reset_index()
                summary = summary.sort_values('post_number')
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
                
                # Estadísticas por plataforma
                platform_stats = df[df['comment_text'].notna()].groupby('platform').agg(
                    Total_Posts=('post_url', 'nunique'),
                    Total_Comentarios=('comment_text', 'count'),
                    Promedio_Likes=('likes_count', 'mean'),
                    Total_Likes=('likes_count', 'sum')
                ).round(2).reset_index()
                platform_stats.to_excel(writer, sheet_name='Stats_Plataforma', index=False)
                
                # URLs con problemas
                if scraper and scraper.failed_urls:
                    failed_df = pd.DataFrame({
                        'URL': scraper.failed_urls,
                        'Status': 'FAILED_ALL_ATTEMPTS'
                    })
                    failed_df.to_excel(writer, sheet_name='URLs_Fallidas', index=False)
        
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False


def load_existing_comments(filename):
    """Carga los comentarios existentes del archivo Excel"""
    if not Path(filename).exists():
        logger.info(f"No existing file found: {filename}. Will create new file.")
        return pd.DataFrame()
    
    try:
        df_existing = pd.read_excel(filename, sheet_name='Comentarios')
        logger.info(f"Loaded {len(df_existing)} existing rows from {filename}")
        
        # Normalizar cadenas vacías a NaN
        if 'comment_text' in df_existing.columns:
            df_existing['comment_text'] = df_existing['comment_text'].replace('', pd.NA)
            df_existing['comment_text'] = df_existing['comment_text'].apply(
                lambda x: pd.NA if isinstance(x, str) and x.strip() == '' else x
            )
        
        # Crear post_url_original si no existe
        if 'post_url_original' not in df_existing.columns:
            df_existing['post_url_original'] = df_existing['post_url'].copy()
        
        return df_existing
    except Exception as e:
        logger.error(f"Error loading existing file: {e}")
        return pd.DataFrame()


def process_datetime_columns(df):
    """Procesa las columnas de fecha/hora"""
    if 'created_time' not in df.columns:
        return df
    
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    
    return df


def run_extraction():
    """Función principal que ejecuta todo el proceso de extracción"""
    logger.info("=" * 60)
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    logger.info(f"--- Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logger.info("=" * 60)
    
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    logger.info(f"URLs to process: {len(valid_urls)}")
    
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    filename = "Comentarios Campaña.xlsx"
    df_existing = load_existing_comments(filename)
    
    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    
    # Mapeo consistente de URLs a números de post
    url_to_post_number = {}
    
    # Si hay datos existentes, preservar la numeración existente
    if not df_existing.empty and 'post_number' in df_existing.columns:
        for url in df_existing['post_url'].unique():
            if pd.notna(url):
                existing_numbers = df_existing[df_existing['post_url'] == url]['post_number'].dropna()
                if not existing_numbers.empty:
                    url_to_post_number[url] = int(existing_numbers.mode().iloc[0])
    
    # Asignar números a URLs nuevas
    next_number = max(url_to_post_number.values()) + 1 if url_to_post_number else 1
    for url in valid_urls:
        if url not in url_to_post_number:
            url_to_post_number[url] = next_number
            next_number += 1
    
    # Procesar cada URL
    for idx, url in enumerate(valid_urls, 1):
        post_number = url_to_post_number[url]
        platform = scraper.detect_platform(url)
        
        if not platform:
            logger.warning(f"Could not detect platform for URL: {url}")
            continue
        
        logger.info(f"\n--- Processing URL {idx}/{len(valid_urls)} (Post #{post_number}) ---")
        
        comments = []
        
        # Usar reintentos automáticos
        if platform == 'facebook':
            comments = scraper.scrape_with_retry(
                scraper.scrape_facebook_comments, 
                url, 500, CAMPAIGN_INFO, post_number
            )
        elif platform == 'instagram':
            comments = scraper.scrape_with_retry(
                scraper.scrape_instagram_comments, 
                url, 500, CAMPAIGN_INFO, post_number
            )
        elif platform == 'tiktok':
            comments = scraper.scrape_with_retry(
                scraper.scrape_tiktok_comments, 
                url, 500, CAMPAIGN_INFO, post_number
            )
        
        # Verificar si la URL falló completamente
        if url in scraper.failed_urls:
            # Crear entrada indicando que falló
            failed_entry = create_failed_registry_entry(url, platform, CAMPAIGN_INFO, post_number)
            all_comments.append(failed_entry)
        elif not comments:
            # No hubo comentarios pero la extracción fue exitosa
            registry_entry = create_post_registry_entry(url, platform, CAMPAIGN_INFO, post_number)
            all_comments.append(registry_entry)
        else:
            # Agregar todos los comentarios
            all_comments.extend(comments)
        
        # Pausa entre URLs (excepto la última)
        if not SOLO_PRIMER_POST and idx < len(valid_urls):
            pausa = random.uniform(30, 60)  # Reducida para ser más eficiente
            logger.info(f"Pausing for {pausa:.2f} seconds before next URL...")
            time.sleep(pausa)

    # Procesar y guardar resultados
    if all_comments:
        df_new_comments = pd.DataFrame(all_comments)
        df_new_comments = process_datetime_columns(df_new_comments)
        
        # Combinar con existentes
        df_combined = merge_comments(df_existing, df_new_comments)
        
        # Ordenar por fecha (más recientes primero)
        if 'created_time_processed' in df_combined.columns:
            df_combined = df_combined.sort_values(
                ['post_number', 'created_time_processed'], 
                ascending=[True, False],
                na_position='last'
            )
        
        # Organizar columnas
        final_columns = [
            'post_number', 'platform', 'campaign_name', 'post_url', 'post_url_original',
            'author_name', 'comment_text', 'created_time_processed', 
            'fecha_comentario', 'hora_comentario', 'likes_count', 
            'replies_count', 'is_reply', 'author_url', 'extraction_status', 'created_time_raw'
        ]
        existing_cols = [col for col in final_columns if col in df_combined.columns]
        df_combined = df_combined[existing_cols]
        
        # Guardar
        save_to_excel(df_combined, filename, scraper)
        
        # Estadísticas finales
        total_comments = df_combined['comment_text'].notna().sum()
        total_posts = df_combined['post_number'].nunique()
        failed_count = len(scraper.failed_urls)
        
        logger.info("=" * 60)
        logger.info("--- EXTRACTION PROCESS FINISHED ---")
        logger.info(f"--- End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        logger.info(f"Total unique posts tracked: {total_posts}")
        logger.info(f"Total comments extracted: {total_comments}")
        if failed_count > 0:
            logger.warning(f"Failed URLs: {failed_count}")
            for failed_url in scraper.failed_urls:
                logger.warning(f"  - {failed_url}")
        logger.info(f"File saved: {filename}")
        logger.info("=" * 60)
    else:
        logger.warning("No new data to process")
        if not df_existing.empty:
            save_to_excel(df_existing, filename)


if __name__ == "__main__":
    run_extraction()

