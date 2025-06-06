#!/usr/bin/env python3
"""
Pocket to Notion Importer (CSV版)
PocketのCSVエクスポートファイルをNotionデータベースに取り込むスクリプト

Author: Assistant
Created: 2025
License: MIT
"""

import os
import time
import zipfile
from typing import List, Dict, Optional, Union, Any
from datetime import datetime
import logging
import pandas as pd

from notion_client import Client
from notion_client.errors import APIResponseError, RequestTimeoutError
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger: logging.Logger = logging.getLogger(__name__)


class PocketToNotionImporter:
    """
    PocketのCSVエクスポートデータをNotionデータベースに取り込むためのクラス
    
    Attributes:
        notion (Client): Notion APIクライアント
        database_id (str): 対象となるNotionデータベースのID
        imported_count (int): 正常にインポートされた記事数
        error_count (int): エラーが発生した記事数
    """
    
    def __init__(self, notion_token: str, database_id: str) -> None:
        """
        PocketToNotionImporterを初期化する
        
        Args:
            notion_token (str): Notion Integration Token
            database_id (str): NotionデータベースのID
            
        Raises:
            ValueError: tokenまたはdatabase_idが空の場合
        """
        if not notion_token:
            raise ValueError("Notion tokenが指定されていません")
        if not database_id:
            raise ValueError("Database IDが指定されていません")
            
        self.notion: Client = Client(auth=notion_token)
        self.database_id: str = database_id
        self.imported_count: int = 0
        self.error_count: int = 0
        self.available_properties: set = set()  # 利用可能なプロパティを保存
    
    def extract_csv_from_zip(self, zip_file_path: str) -> List[str]:
        """
        ZIPファイルからCSVファイルを抽出する
        
        Args:
            zip_file_path (str): PocketエクスポートZIPファイルのパス
            
        Returns:
            List[str]: 抽出されたCSVファイルのパスリスト
            
        Raises:
            FileNotFoundError: ZIPファイルが存在しない場合
            zipfile.BadZipFile: 無効なZIPファイルの場合
        """
        extracted_files: List[str] = []
        
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                # 一時ディレクトリに展開
                extract_dir = os.path.dirname(zip_file_path)
                zip_ref.extractall(extract_dir)
                
                # CSVファイルを探す
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.csv'):
                        full_path = os.path.join(extract_dir, file_name)
                        extracted_files.append(full_path)
                        
            logger.info(f"ZIPファイルから{len(extracted_files)}個のCSVファイルを抽出しました")
            return extracted_files
            
        except FileNotFoundError:
            logger.error(f"ZIPファイルが見つかりません: {zip_file_path}")
            raise
        except zipfile.BadZipFile:
            logger.error(f"無効なZIPファイルです: {zip_file_path}")
            raise
        except Exception as e:
            logger.error(f"ZIPファイルの展開中にエラーが発生しました: {str(e)}")
            raise
    
    def parse_pocket_csv(self, csv_file_path: str) -> List[Dict[str, Any]]:
        """
        PocketのCSVファイルを解析して記事情報を抽出する
        
        Args:
            csv_file_path (str): PocketエクスポートCSVファイルのパス
            
        Returns:
            List[Dict[str, Any]]: 記事情報のリスト。各辞書には以下のキーが含まれる:
                - title (str): 記事のタイトル
                - url (str): 記事のURL
                - tags (List[str]): タグのリスト
                - added_date (Optional[datetime]): 追加日時
                - time_added (Optional[str]): Unix timestamp文字列
                - status (str): ステータス
                
        Raises:
            FileNotFoundError: 指定されたファイルが存在しない場合
            pd.errors.EmptyDataError: CSVファイルが空の場合
            Exception: その他のファイル読み取りエラー
        """
        try:
            # CSVファイルを読み込み（様々なエンコーディングに対応）
            try:
                df = pd.read_csv(csv_file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(csv_file_path, encoding='cp1252')
                except UnicodeDecodeError:
                    df = pd.read_csv(csv_file_path, encoding='shift_jis')
            
            articles: List[Dict[str, Any]] = []
            
            # 各行を処理
            for _, row in df.iterrows():
                # NaN値をNoneまたは空文字列に変換
                title = row.get('title', '')
                if pd.isna(title) or title == '':
                    title = row.get('url', '')  # タイトルが空の場合はURLを使用
                
                url = row.get('url', '')
                if pd.isna(url):
                    continue  # URLが無い行はスキップ
                
                article: Dict[str, Any] = {
                    'title': str(title),
                    'url': str(url),
                    'tags': [],
                    'added_date': None,
                    'time_added': None,
                    'status': str(row.get('status', 'unread'))
                }
                
                # タイムスタンプを処理
                time_added = row.get('time_added')
                if not pd.isna(time_added):
                    try:
                        timestamp = int(float(time_added))
                        article['added_date'] = datetime.fromtimestamp(timestamp)
                        article['time_added'] = str(timestamp)
                    except (ValueError, TypeError, OSError) as e:
                        logger.warning(f"タイムスタンプの変換に失敗しました: {time_added}, エラー: {str(e)}")
                
                # タグを処理
                tags = row.get('tags')
                if not pd.isna(tags) and tags != '':
                    # タグは単一の文字列として格納されている場合が多い
                    tag_str = str(tags).strip()
                    if tag_str:
                        # カンマ区切りの場合とスペース区切りの場合に対応
                        if ',' in tag_str:
                            article['tags'] = [tag.strip() for tag in tag_str.split(',') if tag.strip()]
                        else:
                            article['tags'] = [tag_str]
                
                articles.append(article)
            
            logger.info(f"CSVファイルから{len(articles)}件の記事を解析しました")
            return articles
            
        except FileNotFoundError:
            logger.error(f"CSVファイルが見つかりません: {csv_file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"CSVファイルが空です: {csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"CSVファイルの解析中にエラーが発生しました: {str(e)}")
            raise
    
    def create_notion_page(self, article: Dict[str, Any]) -> bool:
        """
        Notionデータベースに記事ページを作成する
        
        Args:
            article (Dict[str, Any]): 記事情報を含む辞書
            
        Returns:
            bool: 成功した場合True、失敗した場合False
            
        Note:
            Notionの制限に合わせて、タイトルは100文字、タグは10個まで、
            タグ名は100文字までに制限される。
            存在しないプロパティは自動的にスキップされる
        """
        try:
            # URLからドメインを抽出
            domain = ''
            try:
                from urllib.parse import urlparse
                parsed_url = urlparse(article['url'])
                domain = parsed_url.netloc
            except Exception:
                domain = ''
            
            # 必須プロパティを構築
            properties: Dict[str, Any] = {
                "Title": {
                    "title": [
                        {
                            "text": {
                                "content": article['title'][:100]  # Notionの制限に合わせて短縮
                            }
                        }
                    ]
                },
                "URL": {
                    "url": article['url']
                },
                "Domain": {
                    "rich_text": [
                        {
                            "text": {
                                "content": domain
                            }
                        }
                    ]
                },
                "Source": {
                    "select": {
                        "name": "Pocket"
                    }
                }
            }
            
            # オプションプロパティを存在確認してから追加
            
            # Status プロパティ（Pocketでの元ステータス）
            if "Status" in self.available_properties:
                properties["Status"] = {
                    "select": {
                        "name": article.get('status', 'unread').capitalize()
                    }
                }
            
            # ReadingStatus プロパティ（Notionでの読了管理、デフォルトは「未読」）
            if "ReadingStatus" in self.available_properties:
                properties["ReadingStatus"] = {
                    "select": {
                        "name": "未読"
                    }
                }
            
            # 追加日時があり、プロパティが存在する場合のみ設定
            if article.get('added_date') and "AddedDate" in self.available_properties:
                properties["AddedDate"] = {
                    "date": {
                        "start": article['added_date'].isoformat()
                    }
                }
            
            # タグがあり、プロパティが存在する場合のみ設定
            if article.get('tags') and "Tags" in self.available_properties:
                properties["Tags"] = {
                    "multi_select": [
                        {"name": tag[:100]} for tag in article['tags'][:10]  # 最大10個のタグ
                    ]
                }
            
            # Rating プロパティは存在確認のみ（初期値は空のまま、後で手動評価）
            # if "Rating" in self.available_properties:
            #     # 評価は読後に手動で設定するため、初期状態では空のまま
            #     pass
            
            # ページを作成
            response: Dict[str, Any] = self.notion.pages.create(
                parent={"database_id": self.database_id},
                properties=properties
            )
            
            self.imported_count += 1
            logger.info(f"インポート成功: {article['title'][:50]}...")
            return True
            
        except APIResponseError as e:
            self.error_count += 1
            logger.error(f"Notion API エラー - 記事 '{article['title'][:50]}...': {str(e)}")
            return False
        except RequestTimeoutError as e:
            self.error_count += 1
            logger.error(f"タイムアウトエラー - 記事 '{article['title'][:50]}...': {str(e)}")
            return False
        except Exception as e:
            self.error_count += 1
            logger.error(f"予期しないエラー - 記事 '{article['title'][:50]}...': {str(e)}")
            return False
    
    def check_database_properties(self) -> bool:
        """
        Notionデータベースのプロパティが正しく設定されているかを確認する
        
        Returns:
            bool: 必要なプロパティが全て存在する場合True、そうでなければFalse
            
        Note:
            必須プロパティ: Title, URL, Domain, Source
            オプションプロパティ: Status, AddedDate, Tags, ReadingStatus, Rating
        """
        try:
            database: Dict[str, Any] = self.notion.databases.retrieve(self.database_id)
            properties: Dict[str, Any] = database['properties']
            
            # 利用可能なプロパティを保存（後で使用）
            self.available_properties = set(properties.keys())
            
            required_props: List[str] = ['Title', 'URL', 'Domain', 'Source']
            optional_props: List[str] = ['Status', 'AddedDate', 'Tags', 'ReadingStatus', 'Rating']
            
            missing_props: List[str] = []
            for prop in required_props:
                if prop not in properties:
                    missing_props.append(prop)
            
            if missing_props:
                logger.error(f"必須プロパティが不足しています: {', '.join(missing_props)}")
                logger.info("Notionデータベースに以下のプロパティを作成してください:")
                logger.info("- Title (Title)")
                logger.info("- URL (URL)")
                logger.info("- Domain (Text)")
                logger.info("- Source (Select)")
                logger.info("オプションプロパティ:")
                logger.info("- Status (Select) - Pocketでの元ステータス")
                logger.info("- AddedDate (Date)")
                logger.info("- Tags (Multi-select)")
                logger.info("- ReadingStatus (Select) - Notionでの読了管理")
                logger.info("- Rating (Select) - 記事の評価（星1-5）")
                return False
            
            # オプションプロパティの存在確認とログ出力
            available_optional = [prop for prop in optional_props if prop in properties]
            missing_optional = [prop for prop in optional_props if prop not in properties]
            
            if available_optional:
                logger.info(f"利用可能なオプションプロパティ: {', '.join(available_optional)}")
            if missing_optional:
                logger.info(f"未作成のオプションプロパティ: {', '.join(missing_optional)} (これらはスキップされます)")
            
            logger.info("データベースプロパティの確認が完了しました")
            return True
            
        except APIResponseError as e:
            logger.error(f"データベース情報の取得に失敗しました: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"データベースプロパティの確認中にエラーが発生しました: {str(e)}")
            return False
    
    def import_articles(self, file_path: str, delay: Optional[float] = None) -> Dict[str, Union[bool, int, str]]:
        """
        記事をPocketからNotionにインポートする
        
        Args:
            file_path (str): Pocketエクスポートファイルのパス（CSVまたはZIP）
            delay (Optional[float]): API呼び出し間の待機時間（秒）。
                                   Noneの場合は環境変数またはデフォルト値を使用
            
        Returns:
            Dict[str, Union[bool, int, str]]: インポート結果の統計情報
                - success (bool): 全体的な成功/失敗
                - total_articles (int): 総記事数
                - imported (int): 成功したインポート数
                - errors (int): エラー数
                - success_rate (str): 成功率（パーセンテージ）
                - error (str): エラーメッセージ（失敗時のみ）
        """
        # API呼び出し間隔を設定（環境変数 > 引数 > デフォルト値の優先順位）
        if delay is None:
            try:
                delay = float(os.getenv('API_DELAY', '0.3'))
            except ValueError:
                logger.warning("API_DELAYの値が無効です。デフォルト値0.3を使用します")
                delay = 0.3
        
        # データベースプロパティを確認
        if not self.check_database_properties():
            return {'success': False, 'error': 'データベースプロパティの確認に失敗しました'}
        
        # ファイル形式を判定して記事を取得
        all_articles: List[Dict[str, Any]] = []
        
        try:
            if file_path.endswith('.zip'):
                # ZIPファイルの場合
                csv_files = self.extract_csv_from_zip(file_path)
                for csv_file in csv_files:
                    articles = self.parse_pocket_csv(csv_file)
                    all_articles.extend(articles)
            elif file_path.endswith('.csv'):
                # CSVファイルの場合
                all_articles = self.parse_pocket_csv(file_path)
            else:
                return {'success': False, 'error': 'サポートされていないファイル形式です（.csvまたは.zipのみ）'}
                
        except Exception as e:
            logger.error(f"ファイルの解析に失敗しました: {str(e)}")
            return {'success': False, 'error': f'ファイルの解析に失敗しました: {str(e)}'}
        
        if not all_articles:
            logger.warning("ファイルから記事が見つかりませんでした")
            return {'success': False, 'error': '記事が見つかりませんでした'}
        
        # 記事を一つずつインポート
        logger.info(f"{len(all_articles)}件の記事のインポートを開始します...")
        
        for i, article in enumerate(all_articles, 1):
            logger.info(f"記事 {i}/{len(all_articles)} を処理中")
            
            success: bool = self.create_notion_page(article)
            
            # レート制限を避けるための待機
            if i < len(all_articles):  # 最後の記事の後は待機しない
                time.sleep(delay)
        
        # 結果を返す
        success_rate: float = (self.imported_count / len(all_articles)) * 100
        result: Dict[str, Union[bool, int, str]] = {
            'success': True,
            'total_articles': len(all_articles),
            'imported': self.imported_count,
            'errors': self.error_count,
            'success_rate': f"{success_rate:.1f}%"
        }
        
        logger.info(f"インポート完了: {result}")
        return result


def main() -> None:
    """
    メイン実行関数
    
    環境変数から設定を読み込み、PocketからNotionへのインポートを実行する
    必要な環境変数が設定されていない場合は、エラーメッセージを表示して終了する
    """
    # 設定を.envファイルから読み込み
    notion_token: Optional[str] = os.getenv('NOTION_TOKEN')
    database_id: Optional[str] = os.getenv('NOTION_DATABASE_ID')
    file_path: str = os.getenv('POCKET_FILE', 'pocket.zip')
    
    # 設定の確認
    if not notion_token:
        print("エラー: NOTION_TOKENが設定されていません")
        print(".envファイルにNotion Integration Tokenを設定してください")
        print("例: NOTION_TOKEN=secret_your_token_here")
        return
    
    if not database_id:
        print("エラー: NOTION_DATABASE_IDが設定されていません")
        print(".envファイルにNotionデータベースIDを設定してください")
        print("例: NOTION_DATABASE_ID=your_database_id_here")
        return
    
    if not os.path.exists(file_path):
        print(f"エラー: ファイルが見つかりません: {file_path}")
        print("Pocketからエクスポートしたファイル（pocket.zipまたは.csv）を同じディレクトリに配置してください")
        print("または.envファイルでファイルパスを設定してください: POCKET_FILE=path/to/your/file")
        return
    
    # インポーターを初期化して実行
    try:
        importer: PocketToNotionImporter = PocketToNotionImporter(notion_token, database_id)
        result: Dict[str, Union[bool, int, str]] = importer.import_articles(file_path)
        
        if result['success']:
            print(f"\nインポートが正常に完了しました")
            print(f"総記事数: {result['total_articles']}")
            print(f"インポート成功: {result['imported']}")
            print(f"エラー: {result['errors']}")
            print(f"成功率: {result['success_rate']}")
        else:
            print(f"\nインポートに失敗しました: {result['error']}")
            
    except ValueError as e:
        print(f"設定エラー: {str(e)}")
    except Exception as e:
        print(f"予期しないエラーが発生しました: {str(e)}")
        logger.exception("詳細なエラー情報:")


if __name__ == "__main__":
    main()
