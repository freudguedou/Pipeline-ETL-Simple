# 🔄 Pipeline ETL Simple

**Auteur:** Freud GUEDOU  
**Date:** Octobre 2024

## 📋 Description

Pipeline ETL (Extract, Transform, Load) automatisé en Python qui extrait des données depuis des fichiers CSV, les transforme, les valide et les charge dans une base de données SQLite. Ce projet démontre les concepts fondamentaux de l'intégration de données et de la Business Intelligence.

## 🎯 Objectifs

- ✅ **Extraction** : Lecture automatique de fichiers CSV
- ✅ **Transformation** : Nettoyage, validation et enrichissement des données
- ✅ **Chargement** : Insertion dans une base de données SQLite
- ✅ **Validation** : Vérification de l'intégrité des données
- ✅ **Logging** : Traçabilité complète des opérations
- ✅ **Requêtes** : Analyse des données chargées

## 🛠️ Technologies utilisées

- **Python 3.8+** - Langage principal
- **Pandas** - Manipulation de données
- **SQLite3** - Base de données embarquée
- **Logging** - Traçabilité des opérations

## 📊 Fonctionnalités du Pipeline

### Extraction (Extract)
- Lecture de fichiers CSV avec gestion d'encodage
- Détection automatique des colonnes
- Logging des données extraites

### Transformation (Transform)
- **Nettoyage** : Suppression des doublons et valeurs nulles
- **Validation** : Vérification des formats (email, âge, etc.)
- **Transformation** : Conversion de types, normalisation
- **Enrichissement** : Calculs de champs dérivés

### Chargement (Load)
- Insertion dans SQLite
- Création automatique de tables
- Indexation pour performances
- Gestion des erreurs

## 🚀 Installation

```bash
# Cloner le dépôt
git clone https://github.com/votre-username/pipeline-etl-simple.git
cd pipeline-etl-simple

# Créer un environnement virtuel (recommandé)
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

## 💻 Utilisation

### Étape 1 : Générer des données d'exemple

```bash
python generate_sample_data.py
```

Crée 2 fichiers CSV :
- `data/clients.csv` (1000+ lignes)
- `data/ventes.csv` (500 lignes)

### Étape 2 : Exécuter le pipeline ETL

```bash
python etl_pipeline.py
```

Le pipeline va :
1. Extraire les données du CSV
2. Nettoyer et valider les données
3. Transformer selon les règles définies
4. Charger dans la base de données SQLite

### Étape 3 : Analyser les données

```bash
python query_database.py
```

Génère un rapport complet avec :
- Statistiques générales
- Top clients/produits
- Répartitions par catégories

## 📁 Structure du projet

```
pipeline-etl-simple/
│
├── etl_pipeline.py              # Pipeline ETL principal
├── generate_sample_data.py      # Générateur de données test
├── query_database.py            # Requêtes et analyses
├── requirements.txt             # Dépendances Python
├── README.md                    # Documentation
│
├── data/                        # Données CSV (généré)
│   ├── clients.csv
│   └── ventes.csv
│
├── data_warehouse.db            # Base de données SQLite (généré)
└── etl_pipeline.log             # Logs du pipeline (généré)
```

## 📈 Exemple de sortie

### Pipeline ETL
```
======================================================================
🚀 DÉMARRAGE DU PIPELINE ETL
======================================================================
✅ Connexion établie à la base de données: data_warehouse.db
📥 Extraction des données depuis: data/clients.csv
✅ 1050 lignes extraites
🧹 Nettoyage des données...
   ➜ 46 doublons supprimés
✔️  Validation des données...
   ➜ 81 lignes avec format invalide (email)
   ➜ 28 lignes hors plage (age)
🔄 Transformation des données...
   ➜ nom: converti en majuscules
   ➜ email: converti en minuscules
💾 Chargement des données dans la table: clients
✅ 895 lignes chargées dans 'clients'
======================================================================
📊 STATISTIQUES DU PIPELINE
======================================================================
✅ Lignes extraites:    1050
🔄 Lignes transformées: 895
💾 Lignes chargées:     895
❌ Erreurs:             0
⏱️  Durée:               0.05 secondes
======================================================================
```

## 🔧 Personnalisation

### Ajouter des règles de validation

```python
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
```

### Ajouter des transformations

```python
transformations = {
    'nom': {'type': 'uppercase'},
    'email': {'type': 'lowercase'},
    'date_inscription': {'type': 'date'}
}
```

### Traiter vos propres données

```python
pipeline = ETLPipeline('votre_base.db')
pipeline.run_pipeline(
    csv_file='vos_donnees.csv',
    table_name='votre_table',
    validation_rules=vos_regles,
    transformations=vos_transformations
)
```

## 📊 Requêtes SQL disponibles

Le script `query_database.py` inclut des requêtes prédéfinies :

- **Clients** : Top clients, répartition par ville/statut
- **Ventes** : Chiffre d'affaires, produits populaires
- **Analyses** : Tendances mensuelles, statistiques

## 🎓 Concepts démontrés

### ETL
- Pipeline de données complet
- Extraction depuis sources multiples
- Transformations complexes
- Chargement optimisé

### Data Quality
- Validation de données
- Nettoyage automatique
- Gestion des erreurs

### SQL & Bases de données
- SQLite embarqué
- Création de schémas
- Indexation
- Requêtes analytiques

### Logging & Monitoring
- Traçabilité complète
- Fichiers de logs
- Statistiques détaillées

## 🔍 Cas d'usage

Ce pipeline ETL peut être adapté pour :

- Migration de données entre systèmes
- Consolidation de données de sources multiples
- Automatisation de rapports
- Data warehousing
- Préparation de données pour analyse

## 🤝 Contribution

Les contributions sont bienvenues !

1. Fork le projet
2. Créer une branche (`git checkout -b feature/amelioration`)
3. Commit les changements (`git commit -m 'Ajout fonctionnalité'`)
4. Push vers la branche (`git push origin feature/amelioration`)
5. Ouvrir une Pull Request

## 📝 Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 👤 Auteur

**Freud GUEDOU**
- Projet personnel de Business Intelligence
- Spécialisation : ETL, Data Engineering, Python
- Date : Octobre 2024

## 📚 Ressources

- [Documentation Pandas](https://pandas.pydata.org/)
- [SQLite Tutorial](https://www.sqlitetutorial.net/)
- [Python Logging](https://docs.python.org/3/library/logging.html)

---

*Projet réalisé dans le cadre d'un apprentissage en Business Intelligence et Data Engineering*

**⭐ Si ce projet vous est utile, n'hésitez pas à lui donner une étoile sur GitHub !**
