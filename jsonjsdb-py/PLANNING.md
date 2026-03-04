# jsonjsdb-py - Planning Document

## Objectif

Créer une version Python de jsonjsdb qui supporte la lecture ET l'écriture, tout en restant 100% compatible avec le format de base de données jsonjs (utilisé par la version TypeScript).

## Cas d'utilisation

1. **Lecture** : Charger et requêter une base jsonjs existante
2. **Écriture** : Créer, modifier et supprimer des données (CRUD complet)

---

## Architecture proposée

### Stockage interne : Polars DataFrame

`pl.DataFrame` en interne pour toutes les tables, `TypedDict` en sortie pour l'API. Polars offre mémoire réduite, parsing rapide, filtrage vectorisé et scalabilité sans index manuels.

---

## Structure des modules

```
jsonjsdb-py/
├── src/jsonjsdb/
│   ├── __init__.py          # Exports publics
│   ├── database.py          # Classe principale Jsonjsdb (CRUD, relations)
│   ├── table.py             # Classe Table (wrapper Polars DataFrame)
│   ├── loader.py            # Chargement depuis fichiers JSON → Polars
│   ├── writer.py            # Écriture Polars → fichiers JSON
│   └── types.py             # Type hints (TypedDict, Protocols)
├── tests/
└── PLANNING.md
```

---

## API proposée

### Typage : property-style avec `Table[T]`

Python ne supporte pas les *indexed access types* (`T[K]`) comme TypeScript. Le pattern `db.get("user", id) → User` nécessiterait des `@overload` explicites par table, ce qui est verbeux et fragile.

**Solution retenue** : API property-style avec `Table[T]`, inspirée de pymongo (`db.users.find_one()`). L'utilisateur déclare son schéma via une sous-classe avec des annotations `Table[T]` :

```python
from typing import TypedDict
from jsonjsdb import Jsonjsdb, Table

class User(TypedDict):
    id: str
    name: str
    tag_ids: list[str]  # Many-to-many vers Tag

class MyDB(Jsonjsdb):
    user: Table[User]
    tag: Table[Tag]
```

**Résolution du nom de table** : déduit automatiquement du nom de l'attribut (l'attribut `user` → fichier `user.json`).

**Avantages :**
- Typage complet vérifié par Pylance/mypy (retour, arguments, autocomplétion des champs)
- Découvrabilité : `db.` + Tab → liste de toutes les tables
- 1 ligne par table, zéro boilerplate

**Limitation** : Pas d'autocomplétion sur `having.{table}`. Le retour reste correctement typé.

**Mode non-typé** pour le scripting rapide :

```python
db = Jsonjsdb("path/to/db")
user = db["user"].get("user_1")  # dict[str, Any]
```

### Modes de fonctionnement

```python
# Mode 1: Charger une base existante (path requis, doit exister)
db = MyDB("path/to/db")
db.save()  # Écrit au même path

# Mode 2: Nouvelle base en mémoire (tables vides)
db = MyDB()
db.user.add({...})
db.save("path/to/new_db")  # path obligatoire au premier save()
```

- `MyDB(path)` : charge les fichiers JSON existants, erreur si dossier inexistant
- `MyDB()` : crée une base vide en mémoire, `save(path)` requis ensuite

### CRUD - Opérations de base

```python
# ADD - Objet complet obligatoire
db.user.add({"id": "user_1", "name": "Alice", "tag_ids": []})  # → None
db.user.add_all([...])  # → None

# READ
user = db.user.get("user_1")  # → User | None
users = db.user.all()          # → list[User]

# UPDATE - kwargs pour les champs à modifier
db.user.update("user_1", status="inactive")  # → None

# REMOVE
db.user.remove("user_1")        # → bool (True si supprimé)
db.user.remove_all(["user_2"])  # → int (nombre supprimés)
```

### Filtrage non-relationnel

```python
# Égalité (opérateur par défaut)
active_users = db.user.where("status", "==", "active")

# Comparaisons
adults = db.user.where("age", ">", 18)
recent = db.user.where("created_at", ">=", "2025-01-01")

# Inclusion dans une liste
pending = db.user.where("status", "in", ["active", "pending"])

# Null check (value omis)
no_email = db.user.where("email", "is_null")
```

Opérateurs supportés : `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `is_null`, `is_not_null`.

### Requêtes relationnelles

Une seule syntaxe `having.table(id)` pour toutes les relations (one-to-many ET many-to-many) :

```python
# One-to-many : emails d'un user (email.user_id == "user_1")
emails = db.email.having.user("user_1")

# Many-to-many : users ayant un tag (user.tag_ids contains "tag_1")
users = db.user.having.tag("tag_1")

# Hiérarchie : enfants d'un dossier (folder.parent_id == "folder_1")
children = db.folder.having.parent("folder_1")  # Cas spécial : "parent" → parent_id
```

Détection automatique selon les colonnes (ordre de priorité) :
1. `{target}_id` existe dans la table courante → égalité (`email.user_id == x`)
2. Sinon, `{target}_ids` existe dans la table courante → contains (`user.tag_ids contains x`)
3. Cas spécial `parent` → cherche `parent_id` (self-reference)

---

## Format de fichier

**Compatibilité TypeScript** : même structure de fichiers (`.json`, `.json.js`, `__table__.json`) mais nommage snake_case en Python vs camelCase en TypeScript.

### Format JSON

Liste d'objets avec cellules scalaires uniquement (str, int, float, null) — pas d'array ou d'objet imbriqué. Compatible Excel, CSV, SQLite.

```json
[
  {"id": "user_1", "name": "Alice", "status": "active", "tag_ids": ""},
  {"id": "user_2", "name": "Bob", "status": "active", "tag_ids": "tag_1,tag_2"}
]
```

### Format `.json.js` (compatible TypeScript)

Les fichiers `.json.js` utilisent un format array-of-arrays encapsulé dans une variable JavaScript :

```javascript
jsonjs.data['user'] = [['id','name','tag_ids'],['1','Alice','tag_1,tag_2'],['2','Bob','tag_3']]
```

Première ligne = noms des colonnes, lignes suivantes = valeurs. Généré automatiquement au `save()` en parallèle du `.json`.

### Fichier `__table__.json`

Index des tables avec métadonnées :

```json
[
  {"name": "user", "last_modif": 1670156892},
  {"name": "email", "last_modif": 1670156899}
]
```

### Conversion des champs `xxx_ids`

| Fichier JSON | Mémoire Polars | API (TypedDict) |
|--------------|----------------|----------------|
| `"tag_1,tag_2"` | `pl.List(pl.Utf8)` | `list[str]` |
| `""` ou `null` | `[]` | `[]` |

Conversion au chargement : split par `,` → liste. Conversion au save : join par `,` → string.

### Conventions de nommage des relations

| Convention | Signification | Exemple |
|------------|---------------|---------|
| `xxx_id` | Foreign key vers table `xxx` (one-to-many) | `user_id` → table `user` |
| `xxx_ids` | Many-to-many (IDs séparés par virgules) | `tag_ids: "tag_1,tag_2"` |
| `parent_id` | Relation hiérarchique (self-reference) | Catégories imbriquées |

---

## Considérations techniques

- **IDs** : Toujours `str`. Conversion automatique au chargement si numérique.
- **Inférence de types** : Types des colonnes déduits de la première ligne JSON (`int`, `float`, `str`, `null`).
- **Nommage** : snake_case partout (fichiers et API Python).
- **Typage** : `Table[T]` generic avec `T` = `TypedDict`. Vérifié statiquement par Pylance/mypy.

### Comportement sur erreurs

| Opération | Situation | Comportement |
|-----------|-----------|-------------|
| `Jsonjsdb(path)` | Dossier inexistant | `FileNotFoundError` |
| `db["xxx"]` | Table inexistante | `KeyError` |
| `add(row)` | `id` manquant | `ValueError` |
| `add(row)` | `id` déjà existant | `ValueError` |
| `update(id, ...)` | `id` inexistant | `KeyError` |
| `remove(id)` | `id` inexistant | Retourne `False` (no-op) |
| `save()` | Aucun path (init ou save) | `ValueError` |

---

## Détails d'implémentation

### Introspection des annotations

`Jsonjsdb.__init__` utilise `typing.get_type_hints(self.__class__)` pour découvrir les attributs annotés `Table[T]`. Pour chaque attribut :
1. Instancie `Table(name, db_ref)` où `name` = nom de l'attribut
2. Charge les données depuis `{path}/{name}.json` si le fichier existe
3. Assigne l'instance à `self.{name}`

### Proxy having

`Table.having` retourne un objet `HavingProxy(table, db)` avec `__getattr__` :
1. `having.{target}(id)` → cherche `{target}_id` dans la table courante
2. Si trouvé → `filter(col("{target}_id") == id)`
3. Sinon → cherche `{target}_ids` dans la table courante
4. Si trouvé → `filter(col("{target}_ids").list.contains(id))`
5. Sinon → `AttributeError`

---

## Hors scope

- Migrations de schéma
- Accès concurrent / verrouillage de fichiers
- Requêtes chaînées / lazy evaluation
- Transactions / rollback

---

## Plan d'implémentation

### Phase 1 : Lecture

- [ ] `types.py` : Types de base (TableRow, ID)
- [ ] `loader.py` : Chargement JSON → Polars DataFrame
- [ ] `table.py` : Classe Table (get, all, where)
- [ ] `database.py` : Classe Jsonjsdb (init avec/sans path, accès `db["table"]`)

### Phase 2 : Écriture + CRUD

- [ ] `add()`, `add_all()`, `update()`, `remove()`, `remove_all()`
- [ ] `writer.py` : Écriture Polars → JSON + JSON.js
- [ ] `save()` avec génération automatique de `__table__.json`

### Phase 3 : Relations + Intégrité

- [ ] Proxy `having` avec détection auto one-to-many / many-to-many
- [ ] `integrity.py` : Port de IntegrityChecker (validation optionnelle)

---

## Références

- [jsonjsdb (version JS) source](../jsonjsdb/src/)
- [Données de test jsonjsdb-py](./tests/db/)
