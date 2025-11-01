"""
Pipeline ETL Simple - Extraction, Transformation, Chargement
Auteur: Freud GUEDOU
Date: Octobre 2024

Pipeline automatisé qui extrait des données CSV, les transforme, 
les valide et les charge dans une base de données SQLite.
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import os
import re

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

class ETLPipeline:
    """Classe principale du pipeline ETL"""
    
    def __init__(self, db_name='data_warehouse.db'):
        """
        Initialise le pipeline ETL
        
        Args:
            db_name (str): Nom de la base de données SQLite
        """
        self.db_name = db_name
        self.connection = None
        self.stats = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': 0
        }
        
    def connect_db(self):
        """Établit la connexion à la base de données"""
        try:
            self.connection = sqlite3.connect(self.db_name)
            logging.info(f"✅ Connexion établie à la base de données: {self.db_name}")
        except Exception as e:
            logging.error(f"❌ Erreur de connexion à la base de données: {e}")
            raise
    
    def close_db(self):
        """Ferme la connexion à la base de données"""
        if self.connection:
            self.connection.close()
            logging.info("🔒 Connexion à la base de données fermée")
    
    # ==================== EXTRACTION ====================
    
    def extract_csv(self, file_path, encoding='utf-8'):
        """
        Extrait les données d'un fichier CSV
        
        Args:
            file_path (str): Chemin du fichier CSV
            encoding (str): Encodage du fichier
            
        Returns:
            pd.DataFrame: DataFrame contenant les données extraites
        """
        try:
            logging.info(f"📥 Extraction des données depuis: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Fichier non trouvé: {file_path}")
            
            df = pd.read_csv(file_path, encoding=encoding)
            self.stats['extracted'] = len(df)
            
            logging.info(f"✅ {len(df)} lignes extraites")
            logging.info(f"📊 Colonnes: {', '.join(df.columns.tolist())}")
            
            return df
            
        except Exception as e:
            logging.error(f"❌ Erreur lors de l'extraction: {e}")
            self.stats['errors'] += 1
            raise
    
    # ==================== TRANSFORMATION ====================
    
    def clean_data(self, df):
        """
        Nettoie les données (suppression des doublons, valeurs nulles)
        
        Args:
            df (pd.DataFrame): DataFrame à nettoyer
            
        Returns:
            pd.DataFrame: DataFrame nettoyé
        """
        logging.info("🧹 Nettoyage des données...")
        
        initial_count = len(df)
        
        # Supprimer les doublons
        df_clean = df.drop_duplicates()
        duplicates_removed = initial_count - len(df_clean)
        
        if duplicates_removed > 0:
            logging.info(f"   ➜ {duplicates_removed} doublons supprimés")
        
        # Compter les valeurs nulles
        null_counts = df_clean.isnull().sum()
        if null_counts.sum() > 0:
            logging.info(f"   ➜ Valeurs nulles détectées: {null_counts[null_counts > 0].to_dict()}")
        
        return df_clean
    
    def validate_data(self, df, rules):
        """
        Valide les données selon des règles définies
        
        Args:
            df (pd.DataFrame): DataFrame à valider
            rules (dict): Règles de validation
            
        Returns:
            pd.DataFrame: DataFrame validé
        """
        logging.info("✔️  Validation des données...")
        
        df_valid = df.copy()
        rows_before = len(df_valid)
        
        for column, rule in rules.items():
            if column not in df_valid.columns:
                logging.warning(f"   ⚠️  Colonne '{column}' non trouvée")
                continue
            
            if rule['type'] == 'not_null':
                mask = df_valid[column].notna()
                removed = (~mask).sum()
                df_valid = df_valid[mask]
                if removed > 0:
                    logging.info(f"   ➜ {removed} lignes supprimées ({column} null)")
            
            elif rule['type'] == 'range':
                mask = (df_valid[column] >= rule['min']) & (df_valid[column] <= rule['max'])
                removed = (~mask).sum()
                df_valid = df_valid[mask]
                if removed > 0:
                    logging.info(f"   ➜ {removed} lignes hors plage ({column})")
            
            elif rule['type'] == 'pattern':
                mask = df_valid[column].astype(str).str.match(rule['regex'])
                removed = (~mask).sum()
                df_valid = df_valid[mask]
                if removed > 0:
                    logging.info(f"   ➜ {removed} lignes avec format invalide ({column})")
        
        rows_after = len(df_valid)
        total_removed = rows_before - rows_after
        
        if total_removed > 0:
            logging.info(f"   ➜ Total: {total_removed} lignes supprimées après validation")
        
        return df_valid
    
    def transform_data(self, df, transformations):
        """
        Applique des transformations aux données
        
        Args:
            df (pd.DataFrame): DataFrame à transformer
            transformations (dict): Transformations à appliquer
            
        Returns:
            pd.DataFrame: DataFrame transformé
        """
        logging.info("🔄 Transformation des données...")
        
        df_transformed = df.copy()
        
        for column, transform in transformations.items():
            if column not in df_transformed.columns:
                logging.warning(f"   ⚠️  Colonne '{column}' non trouvée")
                continue
            
            if transform['type'] == 'uppercase':
                df_transformed[column] = df_transformed[column].str.upper()
                logging.info(f"   ➜ {column}: converti en majuscules")
            
            elif transform['type'] == 'lowercase':
                df_transformed[column] = df_transformed[column].str.lower()
                logging.info(f"   ➜ {column}: converti en minuscules")
            
            elif transform['type'] == 'date':
                df_transformed[column] = pd.to_datetime(df_transformed[column], errors='coerce')
                logging.info(f"   ➜ {column}: converti en date")
            
            elif transform['type'] == 'category':
                df_transformed[column] = df_transformed[column].astype('category')
                logging.info(f"   ➜ {column}: converti en catégorie")
            
            elif transform['type'] == 'strip':
                df_transformed[column] = df_transformed[column].str.strip()
                logging.info(f"   ➜ {column}: espaces supprimés")
            
            elif transform['type'] == 'calculate':
                df_transformed[column] = eval(transform['formula'])
                logging.info(f"   ➜ {column}: calculé ({transform['formula']})")
        
        self.stats['transformed'] = len(df_transformed)
        return df_transformed
    
    # ==================== CHARGEMENT ====================
    
    def load_to_db(self, df, table_name, if_exists='replace'):
        """
        Charge les données dans la base de données SQLite
        
        Args:
            df (pd.DataFrame): DataFrame à charger
            table_name (str): Nom de la table
            if_exists (str): Action si la table existe ('replace', 'append', 'fail')
        """
        try:
            logging.info(f"💾 Chargement des données dans la table: {table_name}")
            
            df.to_sql(table_name, self.connection, if_exists=if_exists, index=False)
            self.stats['loaded'] = len(df)
            
            logging.info(f"✅ {len(df)} lignes chargées dans '{table_name}'")
            
            # Créer des index pour améliorer les performances
            self.create_indexes(table_name, df.columns.tolist())
            
        except Exception as e:
            logging.error(f"❌ Erreur lors du chargement: {e}")
            self.stats['errors'] += 1
            raise
    
    def create_indexes(self, table_name, columns):
        """
        Crée des index sur les colonnes principales
        
        Args:
            table_name (str): Nom de la table
            columns (list): Liste des colonnes
        """
        try:
            cursor = self.connection.cursor()
            
            # Créer un index sur la première colonne (souvent l'ID)
            if columns:
                index_name = f"idx_{table_name}_{columns[0]}"
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns[0]})")
                logging.info(f"   ➜ Index créé: {index_name}")
            
            self.connection.commit()
            
        except Exception as e:
            logging.warning(f"   ⚠️  Erreur création index: {e}")
    
    # ==================== PIPELINE COMPLET ====================
    
    def run_pipeline(self, csv_file, table_name, validation_rules=None, transformations=None):
        """
        Execute le pipeline ETL complet
        
        Args:
            csv_file (str): Chemin du fichier CSV
            table_name (str): Nom de la table de destination
            validation_rules (dict): Règles de validation (optionnel)
            transformations (dict): Transformations à appliquer (optionnel)
        """
        start_time = datetime.now()
        
        logging.info("="*70)
        logging.info("🚀 DÉMARRAGE DU PIPELINE ETL")
        logging.info("="*70)
        
        try:
            # Connexion à la base de données
            self.connect_db()
            
            # EXTRACT
            df = self.extract_csv(csv_file)
            
            # TRANSFORM
            df = self.clean_data(df)
            
            if validation_rules:
                df = self.validate_data(df, validation_rules)
            
            if transformations:
                df = self.transform_data(df, transformations)
            
            # LOAD
            self.load_to_db(df, table_name)
            
            # Statistiques finales
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logging.info("="*70)
            logging.info("📊 STATISTIQUES DU PIPELINE")
            logging.info("="*70)
            logging.info(f"✅ Lignes extraites:    {self.stats['extracted']}")
            logging.info(f"🔄 Lignes transformées: {self.stats['transformed']}")
            logging.info(f"💾 Lignes chargées:     {self.stats['loaded']}")
            logging.info(f"❌ Erreurs:             {self.stats['errors']}")
            logging.info(f"⏱️  Durée:               {duration:.2f} secondes")
            logging.info("="*70)
            logging.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS!")
            logging.info("="*70)
            
        except Exception as e:
            logging.error(f"❌ PIPELINE ÉCHOUÉ: {e}")
            raise
        
        finally:
            self.close_db()


def main():
    """Fonction principale - exemple d'utilisation"""
    
    # Configuration du pipeline
    pipeline = ETLPipeline(db_name='data_warehouse.db')
    
    # Définir les règles de validation
    validation_rules = {
        'email': {
            'type': 'pattern',
            'regex': r'^[\w\.-]+@[\w\.-]+\.\w+$'
        },
        'age': {
            'type': 'range',
            'min': 18,
            'max': 100
        }
    }
    
    # Définir les transformations
    transformations = {
        'nom': {'type': 'uppercase'},
        'email': {'type': 'lowercase'},
        'ville': {'type': 'strip'},
        'date_inscription': {'type': 'date'}
    }
    
    # Exécuter le pipeline
    pipeline.run_pipeline(
        csv_file='data/clients.csv',
        table_name='clients',
        validation_rules=validation_rules,
        transformations=transformations
    )


if __name__ == "__main__":
    main()
