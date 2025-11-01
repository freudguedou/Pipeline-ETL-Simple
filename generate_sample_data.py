"""
Générateur de Données d'Exemple
Auteur: Freud GUEDOU
Date: Octobre 2024

Génère des fichiers CSV d'exemple pour tester le pipeline ETL
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generer_donnees_clients(nb_lignes=1000):
    """
    Génère un fichier CSV avec des données de clients
    
    Args:
        nb_lignes (int): Nombre de lignes à générer
    """
    print(f"📝 Génération de {nb_lignes} clients...")
    
    # Listes pour générer des données réalistes
    prenoms = ['Jean', 'Marie', 'Pierre', 'Sophie', 'Luc', 'Anne', 'Paul', 'Claire', 
               'Marc', 'Julie', 'Thomas', 'Emma', 'Nicolas', 'Laura', 'David']
    noms = ['Dupont', 'Martin', 'Bernard', 'Dubois', 'Thomas', 'Robert', 'Richard',
            'Petit', 'Durand', 'Leroy', 'Moreau', 'Simon', 'Laurent', 'Lefebvre']
    villes = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Nantes', 'Bordeaux',
              'Lille', 'Rennes', 'Strasbourg']
    
    donnees = []
    
    for i in range(nb_lignes):
        # Données de base
        prenom = random.choice(prenoms)
        nom = random.choice(noms)
        
        # Générer un email (avec quelques erreurs volontaires)
        if random.random() < 0.95:  # 95% d'emails valides
            email = f"{prenom.lower()}.{nom.lower()}@email.com"
        else:  # 5% d'emails invalides
            email = f"{prenom.lower()}{nom.lower()}email.com"  # Sans @
        
        # Âge (avec quelques valeurs hors limites)
        if random.random() < 0.9:  # 90% d'âges valides
            age = random.randint(18, 80)
        else:  # 10% d'âges invalides
            age = random.randint(10, 120)
        
        # Ville (avec espaces pour tester le strip)
        ville = random.choice(villes)
        if random.random() < 0.2:  # 20% avec espaces
            ville = f"  {ville}  "
        
        # Date d'inscription (dernières 2 années)
        jours_avant = random.randint(0, 730)
        date_inscription = (datetime.now() - timedelta(days=jours_avant)).strftime('%Y-%m-%d')
        
        # Montant total dépensé
        montant_total = round(random.uniform(50, 5000), 2)
        
        # Statut client
        statut = random.choice(['Actif', 'Inactif', 'Premium'])
        
        donnees.append({
            'id_client': i + 1,
            'prenom': prenom,
            'nom': nom,
            'email': email,
            'age': age,
            'ville': ville,
            'date_inscription': date_inscription,
            'montant_total': montant_total,
            'statut': statut
        })
    
    # Créer le DataFrame
    df = pd.DataFrame(donnees)
    
    # Ajouter quelques doublons volontaires (5%)
    nb_doublons = int(nb_lignes * 0.05)
    doublons = df.sample(nb_doublons)
    df = pd.concat([df, doublons], ignore_index=True)
    
    # Ajouter quelques valeurs nulles
    for col in ['email', 'ville']:
        null_indices = np.random.choice(df.index, size=int(len(df)*0.02), replace=False)
        df.loc[null_indices, col] = np.nan
    
    return df

def generer_donnees_ventes(nb_lignes=500):
    """
    Génère un fichier CSV avec des données de ventes
    
    Args:
        nb_lignes (int): Nombre de lignes à générer
    """
    print(f"📝 Génération de {nb_lignes} ventes...")
    
    produits = ['Ordinateur', 'Téléphone', 'Tablette', 'Écran', 'Clavier', 
                'Souris', 'Casque', 'Webcam', 'Imprimante', 'Disque dur']
    
    categories = ['Informatique', 'Électronique', 'Accessoires']
    
    donnees = []
    
    for i in range(nb_lignes):
        produit = random.choice(produits)
        
        # Prix unitaire selon le produit
        prix_base = {
            'Ordinateur': 800, 'Téléphone': 600, 'Tablette': 400,
            'Écran': 300, 'Clavier': 50, 'Souris': 30,
            'Casque': 80, 'Webcam': 100, 'Imprimante': 200, 'Disque dur': 100
        }
        
        prix_unitaire = prix_base.get(produit, 100) + random.uniform(-50, 100)
        quantite = random.randint(1, 10)
        montant = round(prix_unitaire * quantite, 2)
        
        # Date de vente
        jours_avant = random.randint(0, 365)
        date_vente = (datetime.now() - timedelta(days=jours_avant)).strftime('%Y-%m-%d')
        
        # ID client (référence aux clients)
        id_client = random.randint(1, 1000)
        
        donnees.append({
            'id_vente': i + 1,
            'id_client': id_client,
            'produit': produit,
            'categorie': random.choice(categories),
            'quantite': quantite,
            'prix_unitaire': round(prix_unitaire, 2),
            'montant': montant,
            'date_vente': date_vente
        })
    
    df = pd.DataFrame(donnees)
    return df

def sauvegarder_donnees():
    """
    Génère et sauvegarde les fichiers CSV
    """
    print("\n" + "="*70)
    print("  GÉNÉRATEUR DE DONNÉES D'EXEMPLE POUR PIPELINE ETL")
    print("  Auteur: Freud GUEDOU | Octobre 2024")
    print("="*70 + "\n")
    
    # Créer le dossier data s'il n'existe pas
    os.makedirs('data', exist_ok=True)
    
    # Générer les données clients
    df_clients = generer_donnees_clients(1000)
    df_clients.to_csv('data/clients.csv', index=False, encoding='utf-8-sig')
    print(f"✅ Fichier créé: data/clients.csv ({len(df_clients)} lignes)")
    
    # Générer les données ventes
    df_ventes = generer_donnees_ventes(500)
    df_ventes.to_csv('data/ventes.csv', index=False, encoding='utf-8-sig')
    print(f"✅ Fichier créé: data/ventes.csv ({len(df_ventes)} lignes)")
    
    print("\n" + "="*70)
    print("📊 APERÇU DES DONNÉES GÉNÉRÉES")
    print("="*70)
    print("\n🔹 Clients (5 premières lignes):")
    print(df_clients.head())
    print(f"\n📈 Statistiques clients:")
    print(f"   • Âge moyen: {df_clients['age'].mean():.1f} ans")
    print(f"   • Montant total moyen: {df_clients['montant_total'].mean():.2f} €")
    print(f"   • Villes représentées: {df_clients['ville'].nunique()}")
    
    print("\n🔹 Ventes (5 premières lignes):")
    print(df_ventes.head())
    print(f"\n📈 Statistiques ventes:")
    print(f"   • Montant moyen: {df_ventes['montant'].mean():.2f} €")
    print(f"   • Quantité moyenne: {df_ventes['quantite'].mean():.1f}")
    print(f"   • Produits différents: {df_ventes['produit'].nunique()}")
    
    print("\n" + "="*70)
    print("✅ GÉNÉRATION TERMINÉE!")
    print("="*70)
    print("\n🎯 Prochaines étapes:")
    print("   1. Exécuter le pipeline ETL: python etl_pipeline.py")
    print("   2. Interroger la base de données: python query_database.py")
    print("\n" + "="*70 + "\n")

if __name__ == "__main__":
    sauvegarder_donnees()
