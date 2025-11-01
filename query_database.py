"""
Requêtes et Analyses de la Base de Données
Auteur: Freud GUEDOU
Date: Octobre 2024

Script pour interroger et analyser les données chargées par le pipeline ETL
"""

import sqlite3
import pandas as pd
from datetime import datetime

class DatabaseAnalyzer:
    """Classe pour analyser les données dans la base de données"""
    
    def __init__(self, db_name='data_warehouse.db'):
        """
        Initialise l'analyseur de base de données
        
        Args:
            db_name (str): Nom de la base de données
        """
        self.db_name = db_name
        self.connection = None
    
    def connect(self):
        """Établit la connexion à la base de données"""
        try:
            self.connection = sqlite3.connect(self.db_name)
            print(f"✅ Connecté à la base de données: {self.db_name}")
        except Exception as e:
            print(f"❌ Erreur de connexion: {e}")
            raise
    
    def close(self):
        """Ferme la connexion"""
        if self.connection:
            self.connection.close()
            print("🔒 Connexion fermée")
    
    def list_tables(self):
        """Liste toutes les tables de la base de données"""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        df = pd.read_sql_query(query, self.connection)
        return df['name'].tolist()
    
    def get_table_info(self, table_name):
        """
        Obtient les informations sur une table
        
        Args:
            table_name (str): Nom de la table
        """
        query = f"PRAGMA table_info({table_name})"
        df = pd.read_sql_query(query, self.connection)
        return df
    
    def get_row_count(self, table_name):
        """
        Compte le nombre de lignes dans une table
        
        Args:
            table_name (str): Nom de la table
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        df = pd.read_sql_query(query, self.connection)
        return df['count'].iloc[0]
    
    # ==================== REQUÊTES CLIENTS ====================
    
    def get_top_clients(self, limit=10):
        """
        Obtient les meilleurs clients par montant total
        
        Args:
            limit (int): Nombre de clients à retourner
        """
        query = f"""
        SELECT id_client, prenom, nom, email, ville, montant_total, statut
        FROM clients
        ORDER BY montant_total DESC
        LIMIT {limit}
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_clients_by_city(self):
        """Obtient le nombre de clients par ville"""
        query = """
        SELECT ville, COUNT(*) as nombre_clients, 
               AVG(montant_total) as montant_moyen
        FROM clients
        GROUP BY ville
        ORDER BY nombre_clients DESC
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_clients_by_status(self):
        """Obtient la répartition des clients par statut"""
        query = """
        SELECT statut, COUNT(*) as nombre_clients,
               SUM(montant_total) as revenu_total,
               AVG(montant_total) as montant_moyen
        FROM clients
        GROUP BY statut
        ORDER BY revenu_total DESC
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_age_statistics(self):
        """Obtient les statistiques d'âge des clients"""
        query = """
        SELECT 
            MIN(age) as age_min,
            MAX(age) as age_max,
            AVG(age) as age_moyen,
            COUNT(*) as total_clients
        FROM clients
        """
        return pd.read_sql_query(query, self.connection)
    
    # ==================== REQUÊTES VENTES ====================
    
    def get_sales_summary(self):
        """Obtient un résumé des ventes"""
        query = """
        SELECT 
            COUNT(*) as nombre_ventes,
            SUM(montant) as chiffre_affaires,
            AVG(montant) as vente_moyenne,
            SUM(quantite) as quantite_totale
        FROM ventes
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_top_products(self, limit=10):
        """
        Obtient les produits les plus vendus
        
        Args:
            limit (int): Nombre de produits à retourner
        """
        query = f"""
        SELECT produit, categorie,
               COUNT(*) as nombre_ventes,
               SUM(quantite) as quantite_totale,
               SUM(montant) as revenu_total,
               AVG(prix_unitaire) as prix_moyen
        FROM ventes
        GROUP BY produit, categorie
        ORDER BY revenu_total DESC
        LIMIT {limit}
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_sales_by_category(self):
        """Obtient les ventes par catégorie"""
        query = """
        SELECT categorie,
               COUNT(*) as nombre_ventes,
               SUM(montant) as revenu_total,
               AVG(montant) as vente_moyenne
        FROM ventes
        GROUP BY categorie
        ORDER BY revenu_total DESC
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_monthly_sales(self):
        """Obtient les ventes par mois"""
        query = """
        SELECT 
            strftime('%Y-%m', date_vente) as mois,
            COUNT(*) as nombre_ventes,
            SUM(montant) as revenu
        FROM ventes
        GROUP BY mois
        ORDER BY mois DESC
        LIMIT 12
        """
        return pd.read_sql_query(query, self.connection)
    
    # ==================== REQUÊTES AVANCÉES ====================
    
    def get_client_purchase_history(self, client_id):
        """
        Obtient l'historique d'achat d'un client
        
        Args:
            client_id (int): ID du client
        """
        query = f"""
        SELECT v.id_vente, v.produit, v.categorie, 
               v.quantite, v.montant, v.date_vente,
               c.prenom, c.nom, c.email
        FROM ventes v
        JOIN clients c ON v.id_client = c.id_client
        WHERE c.id_client = {client_id}
        ORDER BY v.date_vente DESC
        """
        return pd.read_sql_query(query, self.connection)
    
    def get_customers_without_purchases(self):
        """Obtient les clients qui n'ont pas encore acheté"""
        query = """
        SELECT c.id_client, c.prenom, c.nom, c.email, c.statut
        FROM clients c
        LEFT JOIN ventes v ON c.id_client = v.id_client
        WHERE v.id_vente IS NULL
        """
        return pd.read_sql_query(query, self.connection)
    
    # ==================== RAPPORT COMPLET ====================
    
    def generate_full_report(self):
        """Génère un rapport complet de la base de données"""
        print("\n" + "="*70)
        print("📊 RAPPORT D'ANALYSE DE LA BASE DE DONNÉES")
        print("="*70)
        print(f"⏰ Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Tables disponibles
        print("\n📋 TABLES DISPONIBLES")
        print("-" * 70)
        tables = self.list_tables()
        for table in tables:
            row_count = self.get_row_count(table)
            print(f"   • {table}: {row_count} lignes")
        
        # Statistiques clients
        if 'clients' in tables:
            print("\n👥 STATISTIQUES CLIENTS")
            print("-" * 70)
            
            age_stats = self.get_age_statistics()
            print(f"   • Total clients: {age_stats['total_clients'].iloc[0]}")
            print(f"   • Âge moyen: {age_stats['age_moyen'].iloc[0]:.1f} ans")
            print(f"   • Âge min/max: {age_stats['age_min'].iloc[0]} - {age_stats['age_max'].iloc[0]} ans")
            
            print("\n   Top 5 clients:")
            top_clients = self.get_top_clients(5)
            for _, client in top_clients.iterrows():
                print(f"   {client['prenom']} {client['nom']}: {client['montant_total']:.2f} € ({client['statut']})")
            
            print("\n   Clients par statut:")
            status = self.get_clients_by_status()
            for _, row in status.iterrows():
                print(f"   {row['statut']}: {row['nombre_clients']} clients - {row['revenu_total']:.2f} €")
        
        # Statistiques ventes
        if 'ventes' in tables:
            print("\n💰 STATISTIQUES VENTES")
            print("-" * 70)
            
            sales_summary = self.get_sales_summary()
            print(f"   • Total ventes: {sales_summary['nombre_ventes'].iloc[0]}")
            print(f"   • Chiffre d'affaires: {sales_summary['chiffre_affaires'].iloc[0]:.2f} €")
            print(f"   • Vente moyenne: {sales_summary['vente_moyenne'].iloc[0]:.2f} €")
            
            print("\n   Top 5 produits:")
            top_products = self.get_top_products(5)
            for _, product in top_products.iterrows():
                print(f"   {product['produit']}: {product['revenu_total']:.2f} € ({product['nombre_ventes']} ventes)")
            
            print("\n   Ventes par catégorie:")
            categories = self.get_sales_by_category()
            for _, cat in categories.iterrows():
                print(f"   {cat['categorie']}: {cat['revenu_total']:.2f} € ({cat['nombre_ventes']} ventes)")
        
        print("\n" + "="*70)
        print("✅ RAPPORT TERMINÉ")
        print("="*70 + "\n")


def main():
    """Fonction principale"""
    
    analyzer = DatabaseAnalyzer('data_warehouse.db')
    
    try:
        analyzer.connect()
        
        # Générer le rapport complet
        analyzer.generate_full_report()
        
        # Exemples de requêtes personnalisées
        print("📊 EXEMPLES DE REQUÊTES PERSONNALISÉES")
        print("="*70)
        
        print("\n🏙️  Clients par ville:")
        print(analyzer.get_clients_by_city())
        
        print("\n📅 Ventes des 6 derniers mois:")
        print(analyzer.get_monthly_sales())
        
    except FileNotFoundError:
        print("❌ Base de données non trouvée!")
        print("   Exécutez d'abord: python generate_sample_data.py")
        print("   Puis: python etl_pipeline.py")
    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
