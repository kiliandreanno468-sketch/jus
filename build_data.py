#!/usr/bin/env python3
"""Prépare les données de JUS à partir de la base nationale des bornes (IRVE).

Télécharge le fichier consolidé publié chaque jour par transport.data.gouv.fr,
regroupe les points de charge par station, lit les tarifs publiés par les
opérateurs, puis écrit des petites « tuiles » JSON que l'appli charge selon
la zone affichée.

Usage :
    python3 pipeline/build_data.py                      # télécharge la base officielle
    python3 pipeline/build_data.py --input fichier.csv  # utilise un fichier local
Sortie : dist/data/index.json et dist/data/tiles/*.json
"""
import argparse
import csv
import io
import json
import math
import re
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SOURCE = "https://proxy.transport.data.gouv.fr/resource/consolidation-transport-irve-statique"
# Flux national des états des points de charge (libre, occupé, hors service), rafraîchi en temps réel
DYN_SOURCES = [
    "https://transport.data.gouv.fr/resources/84098/download",
    "https://www.data.gouv.fr/api/1/datasets/r/89185b1f-f958-4c5b-9282-399a66ecee97",
]
ZONES = {
    "secteur_cergy": (48.95, 49.12, 1.88, 2.20),
    "paris": (48.80, 48.92, 2.22, 2.47),
}
CELL = 0.25  # taille d'une tuile, en degrés
FRANCE = (41.0, 51.6, -5.5, 10.0)  # lat min, lat max, lon min, lon max (métropole et Corse)

NUM = r"(\d+(?:[.,]\d+)?)"
CUR = r"(?:€|eur(?:os?)?(?![a-z])|e(?![a-zà-ÿ])|cts?(?![a-z])|c€|centimes?)"
KWH = r"(?:kw\s?/\s?h|k\s?w\s?h|kw)(?=(?:ht|ttc)?(?![a-zà-ÿ]))"
NODIGIT = r"(?:(?!kw|min|mn)[^\d\n])"
IDLE = re.compile(r"hors charge|charg[ée]e?\s+mais|est\s+charg[ée]e?\b|apr[eè]s\s+(?:la\s+)?(?:re)?charge|parking|occupation|stationnement|fin de charge|une fois la charge", re.I)
SEP = re.compile(r",\s|;|\||\+")


def fr_num(s):
    return float(s.replace(",", "."))


def clean_text(text):
    t = str(text).replace("\u00a0", " ").replace("\u00e2\u0082\u00ac", "€").replace("â‚¬", "€").replace("\u0080", "€")
    return re.sub(r"\s+", " ", t).strip()


def _segment_end(t, i):
    m = SEP.search(t, i)
    return m.start() if m else len(t)


def _prev_number_end(t, i):
    last = 0
    for m in re.finditer(r"\d", t[:i]):
        last = m.end()
    return last


def _segment_start(t, i):
    last = 0
    for m in SEP.finditer(t[:i]):
        last = m.end()
    return last


def _is_idle(t, m):
    """Vrai si ce prix au temps concerne l'occupation après la charge (contexte limité à sa propre clause)."""
    before = t[max(_prev_number_end(t, m.start()), _segment_start(t, m.start())):m.start()]
    nxt = re.search(r"\d", t[m.end():])
    stop = m.end() + nxt.start() if nxt else len(t)
    after = t[m.end():min(stop, _segment_end(t, m.end()), m.end() + 30)]
    return bool(IDLE.search(before) or IDLE.search(after))


def _after(t, m):
    seg = t[m.end():_segment_end(t, m.end())]
    a = re.search(r"(?:après|apres|au[- ]del[aà] de|à partir de|a partir de)\s*(\d+)\s*(min|h)", seg, re.I)
    return int(a.group(1)) * (60 if a.group(2).lower() == "h" else 1) if a else None


def _value(num, cur):
    v = fr_num(num)
    if cur and re.match(r"c(?:ts?|€|entimes?)", cur, re.I) and v >= 1:
        v = v / 100
    return v


def parse_tarif(text):
    """Extrait €/kWh, €/min (et délai), frais de session et gratuité d'un texte libre d'opérateur."""
    out = {"kwh": None, "min": None, "after": None, "ses": None, "free": False}
    if not text:
        return out
    t = clean_text(text)
    if re.match(r"^\s*gratuit", t, re.I):
        out["free"] = True
        return out
    # Format JSON de certains opérateurs
    if t.startswith("{") and "energyPrice" in t:
        m = re.search(r'"energyPrice"\s*:\s*' + NUM, t)
        if m and 0.03 < fr_num(m.group(1)) < 2:
            out["kwh"] = fr_num(m.group(1))
        m = re.search(r'"fixedPrice"\s*:\s*' + NUM, t)
        if m and 0 < fr_num(m.group(1)) < 20:
            out["ses"] = fr_num(m.group(1))
        return out
    # Prix au kWh
    for m in re.finditer(NUM + r"\s*(" + CUR + r")?\s*(?:ht|ttc)?" + NODIGIT + r"{0,16}?" + KWH, t, re.I):
        v = _value(m.group(1), m.group(2))
        if 0.03 < v < 2:
            out["kwh"] = round(v, 5)
            break
    if out["kwh"] is None:
        m = re.search(r"per\s+kwh[^:]{0,40}:\s*" + NUM + r"\s*(?:€|eur)", t, re.I)
        if m and 0.03 < fr_num(m.group(1)) < 2:
            out["kwh"] = fr_num(m.group(1))
    if out["kwh"] is None:
        m = re.match(r"^\s*" + NUM + r"\s*(?:€|e|eur)?\s*(?:ht|ttc)?\s*$", t, re.I)
        if m and 0.08 <= fr_num(m.group(1)) <= 1.2:
            out["kwh"] = fr_num(m.group(1))
    # Prix pendant la charge, à la minute ou à l'heure (les frais d'occupation après la charge sont ignorés)
    for m in re.finditer(NUM + r"\s*(?:" + CUR + r")?\s*(?:ht|ttc)?\s*(?:ac|dc)?\s*(?:/|par|à la|a la|la)?\s*(?:minute|min|mn)\b", t, re.I):
        if _is_idle(t, m):
            continue
        p = fr_num(m.group(1))
        if 0 < p < 2:
            out["min"] = p
            out["after"] = _after(t, m)
            break
    if out["min"] is None:
        for m in re.finditer(NUM + r"\s*(?:€|eur(?:os?)?|e(?![a-zà-ÿ]))\s*(?:ht|ttc)?\s*(?:/|par|l'|de l')\s*h(?:eure)?\b", t, re.I):
            if _is_idle(t, m):
                continue
            h = fr_num(m.group(1))
            if 0 < h < 60:
                out["min"] = round(h / 60, 3)
                out["after"] = _after(t, m)
                break
    # Frais fixes par session (ignorés s'ils ne valent que sur une plage horaire)
    pats = [
        NUM + r"\s*(?:" + CUR + r")?\s*(?:ht|ttc)?" + NODIGIT + r"{0,25}?(?:session|connexion|d[ée]marrage|lancement|d[ée]part|co[uû]t fixe)",
        r"(?:frais|co[uû]t|prix)\s+(?:fixe\s+)?(?:de\s+)?(?:session|connexion|d[ée]marrage|lancement|d[ée]part)[^\d]{0,30}?" + NUM,
        NUM + r"\s*(?:€|e)\s*\+\s*\d+(?:[.,]\d+)?\s*(?:€|e)?\s*/\s*" + KWH,
    ]
    for pat in pats:
        for m in re.finditer(pat, t, re.I):
            seg = t[_prev_number_end(t, m.start()):_segment_end(t, m.end())]
            if re.search(r"entre\s+\d", seg, re.I) or re.search(r"/\s*" + KWH + r"|/\s*(?:min|mn)\b", t[m.start():m.end()], re.I) and pat is not pats[2]:
                continue
            sv = fr_num(m.group(1))
            if 0 < sv < 20:
                out["ses"] = sv
                break
        if out["ses"] is not None:
            break
    return out


def truthy(v):
    return str(v).strip().lower() in ("true", "vrai", "oui", "1", "yes")


def to_float(v):
    try:
        return float(str(v).replace(",", ".").strip())
    except (TypeError, ValueError):
        return math.nan


def in_france(lat, lon):
    return FRANCE[0] <= lat <= FRANCE[1] and FRANCE[2] <= lon <= FRANCE[3]


def coords(row):
    """Coordonnées fiables : champs consolidés si présents, sinon coordonneesXY ([lon, lat], parfois inversé)."""
    lat, lon = to_float(row.get("consolidated_latitude")), to_float(row.get("consolidated_longitude"))
    if not (math.isnan(lat) or math.isnan(lon)) and in_france(lat, lon):
        return lat, lon
    xy = row.get("coordonneesxy") or ""
    nums = re.findall(r"-?\d+(?:\.\d+)?", xy)
    if len(nums) >= 2:
        a, b = float(nums[0]), float(nums[1])
        if in_france(b, a):
            return b, a
        if in_france(a, b):
            return a, b
    return None


def clean(s, limit=160):
    s = re.sub(r"\s+", " ", str(s or "")).strip()
    return s[:limit]


CITY = r"[A-Za-zÀ-ÿ'’][A-Za-zÀ-ÿ'’ -]{1,}"


def city_from(row):
    """Commune : champ consolidé, sinon après le code postal de l'adresse, sinon en tête du nom de station en majuscules."""
    c = clean(row.get("consolidated_commune"), 60)
    if c:
        return c
    a = clean(row.get("adresse_station"), 200)
    m = re.search(r"\b\d{5}\s+(" + CITY + r")", a)
    if m:
        return m.group(1).strip(" -,")[:60]
    n = clean(row.get("nom_station"), 90)
    m = re.match(r"^([A-ZÀ-Ÿ'’][A-ZÀ-Ÿ'’ -]{2,}?)\s+-\s+", n)
    if m:
        return m.group(1).strip()[:60]
    return ""


FAUX_IDS = {"nonconcerne", "nonapplicable", "na", "nc", "inconnu", "null", "none", "aucun", "sansobjet", "nonrenseigne"}


def cell_key(lat, lon):
    return "%.2f_%.2f" % (math.floor(lat / CELL) * CELL, math.floor(lon / CELL) * CELL)


def build(rows, pdc_map=None):
    stations = {}
    seen_pdc = set()
    for raw in rows:
        row = {(k or "").strip().lower(): v for k, v in raw.items()}
        pos = coords(row)
        if not pos:
            continue
        pdc = row.get("id_pdc_itinerance") or ""
        if pdc and pdc in seen_pdc:
            continue
        if pdc:
            seen_pdc.add(pdc)
        lat, lon = pos
        sid = (row.get("id_station_itinerance") or "").strip()
        # « Non concerné », « NA »… ne sont pas de vrais identifiants : on se rabat sur le nom et la position
        if len(sid) < 3 or re.sub(r"[^a-z]", "", sid.lower().replace("é", "e")) in FAUX_IDS:
            sid = ""
        key = sid or "%s|%.5f|%.5f" % (row.get("nom_station", ""), lat, lon)
        kw = to_float(row.get("puissance_nominale"))
        if not math.isnan(kw) and kw > 1000:
            kw = kw / 1000
        st = stations.get(key)
        if st is None:
            st = stations[key] = {
                "i": re.sub(r"[^\w-]", "", str(key))[:60], "n": clean(row.get("nom_station"), 90),
                "e": clean(row.get("nom_enseigne") or row.get("nom_operateur"), 60), "o": clean(row.get("nom_operateur"), 60),
                "a": clean(row.get("adresse_station"), 120), "c": city_from(row),
                "la": round(lat, 5), "lo": round(lon, 5), "p": 0, "k": set(), "nb": 0, "rows": 0,
                "f": False, "cb": False, "h": clean(row.get("horaires"), 60), "t": "", "pr": None, "prkw": -1,
                "u": clean(row.get("date_maj"), 10),
            }
        st["rows"] += 1
        if pdc_map is not None and pdc:
            pdc_map[pdc] = st
        nb = to_float(row.get("nbre_pdc"))
        if not math.isnan(nb):
            st["nb"] = max(st["nb"], int(nb))
        if not math.isnan(kw):
            st["p"] = max(st["p"], round(kw, 1))
        for field, label in (("prise_type_combo_ccs", "CCS"), ("prise_type_chademo", "CHAdeMO"), ("prise_type_2", "T2"), ("prise_type_ef", "E/F")):
            if truthy(row.get(field)):
                st["k"].add(label)
        if truthy(row.get("gratuit")):
            st["f"] = True
        if truthy(row.get("paiement_cb")):
            st["cb"] = True
        tarif = clean(row.get("tarification"), 200)
        if tarif:
            parsed = parse_tarif(tarif)
            if parsed["free"]:
                st["f"] = True
            if parsed["kwh"] is not None or parsed["min"] is not None:
                # on garde le tarif du point de charge le plus puissant (celui que JUS utilise pour le calcul)
                pkw = 0 if math.isnan(kw) else kw
                if pkw >= st["prkw"]:
                    st["prkw"] = pkw
                    st["pr"] = {k: v for k, v in parsed.items() if k != "free" and v is not None}
                    st["t"] = tarif
            elif not st["t"]:
                st["t"] = tarif

    tiles = {}
    stats = {"stations": 0, "pdc": len(seen_pdc), "priced": 0, "free": 0}
    unparsed, parsed = Counter(), Counter()
    no_text = 0
    for st in stations.values():
        if not st["f"]:
            if st["pr"] is not None:
                parsed[st["t"]] += 1
            elif st["t"]:
                unparsed[st["t"]] += 1
            else:
                no_text += 1
        if not st["nb"]:
            st["nb"] = st["rows"]
        order = ["CCS", "CHAdeMO", "T2", "E/F"]
        st["k"] = [x for x in order if x in st["k"]]
        for tmp in ("rows", "prkw"):
            del st[tmp]
        if st["pr"] is None:
            del st["pr"]
        for k in ("o", "a", "c", "h", "t", "u"):
            if not st[k]:
                del st[k]
        if st.get("o") == st.get("e"):
            st.pop("o", None)
        stats["stations"] += 1
        if st["f"]:
            stats["free"] += 1
        elif "pr" in st:
            stats["priced"] += 1
        tiles.setdefault(cell_key(st["la"], st["lo"]), []).append(st)
    diag = {
        "sans_tarif": no_text, "tarif_illisible": sum(unparsed.values()),
        "tarifs_illisibles_frequents": unparsed.most_common(150),
        "tarifs_lus_frequents": [[t, n, parse_tarif(t)] for t, n in parsed.most_common(40)],
    }
    return tiles, stats, diag


def write(tiles, stats, out_dir, source, diag=None, columns=None):
    out = Path(out_dir)
    (out / "tiles").mkdir(parents=True, exist_ok=True)
    for old in (out / "tiles").glob("*.json"):
        old.unlink()
    for key, items in tiles.items():
        (out / "tiles" / (key + ".json")).write_text(json.dumps(items, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    index = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source, "licence": "Licence Ouverte v2.0 (Etalab)", "cell": CELL,
        "tiles": sorted(tiles.keys()), **stats,
    }
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")
    if diag is not None:
        report = {"generated": index["generated"], "colonnes": columns or [], **stats, **diag}
        (out / "diagnostic.json").write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return index


def parse_ts(v):
    v = (v or "").strip()
    if not v:
        return None
    try:
        d = datetime.fromisoformat(v.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def dispo_diag(text, pdc_map, now=None):
    """Mesure ce que vaut le flux de disponibilité : couverture, fraîcheur, états, par zone et par opérateur."""
    now = now or datetime.now(timezone.utc)
    sample = text[:4096]
    reader = csv.DictReader(io.StringIO(text), delimiter=";" if sample.count(";") > sample.count(",") else ",")
    cols = [(c or "").strip() for c in (reader.fieldnames or [])]
    def col(*names):
        for n in names:
            for c in cols:
                if n in c.lower():
                    return c
        return None
    c_id, c_etat, c_occ, c_ts = col("id_pdc_itinerance", "id_pdc"), col("etat_pdc"), col("occupation_pdc", "occupation"), col("horodatage")
    latest, lignes, exemples = {}, 0, []
    for raw in reader:
        r = {(k or "").strip(): v for k, v in raw.items()}
        lignes += 1
        if len(exemples) < 3:
            exemples.append({k: (v or "")[:40] for k, v in r.items()})
        pdc = (r.get(c_id) or "").strip() if c_id else ""
        if not pdc:
            continue
        ts = parse_ts(r.get(c_ts)) if c_ts else None
        prev = latest.get(pdc)
        if prev is None or (ts and (prev[0] is None or ts > prev[0])):
            latest[pdc] = (ts, (r.get(c_etat) or "").strip().lower(), (r.get(c_occ) or "").strip().lower())
    fraicheur, occupation, etat = Counter(), Counter(), Counter()
    st_any, st_1h, st_10 = set(), set(), set()
    relies = 0
    for pdc, (ts, e, o) in latest.items():
        age = (now - ts).total_seconds() / 60 if ts else None
        b = "sans_date" if age is None else "moins_10_min" if age <= 10 else "moins_1_h" if age <= 60 else "moins_24_h" if age <= 1440 else "plus_ancien"
        fraicheur[b] += 1
        occupation[o or "vide"] += 1
        etat[e or "vide"] += 1
        st = pdc_map.get(pdc)
        if st is None:
            continue
        relies += 1
        st_any.add(st["i"])
        if age is not None and age <= 60:
            st_1h.add(st["i"])
        if age is not None and age <= 10:
            st_10.add(st["i"])
    toutes = {}
    for st in pdc_map.values():
        toutes[st["i"]] = st
    def pct(a, b):
        return round(100 * a / b, 1) if b else 0
    zones = {}
    for name, (la0, la1, lo0, lo1) in ZONES.items():
        ids = [i for i, st in toutes.items() if la0 <= st["la"] <= la1 and lo0 <= st["lo"] <= lo1]
        n1h = sum(1 for i in ids if i in st_1h)
        zones[name] = {"stations": len(ids), "avec_etat": sum(1 for i in ids if i in st_any), "etat_moins_1_h": n1h, "pct_moins_1_h": pct(n1h, len(ids))}
    ops_total, ops_1h = Counter(), Counter()
    for i, st in toutes.items():
        op = st.get("o") or st.get("e") or "Inconnu"
        ops_total[op] += 1
        if i in st_1h:
            ops_1h[op] += 1
    return {
        "genere": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "colonnes": cols, "lignes": lignes, "pdc_uniques": len(latest), "pdc_relies_a_une_station": relies,
        "stations_total": len(toutes), "stations_avec_etat": len(st_any), "stations_etat_moins_1_h": len(st_1h), "stations_etat_moins_10_min": len(st_10),
        "pct_stations_moins_1_h": pct(len(st_1h), len(toutes)), "pct_stations_moins_10_min": pct(len(st_10), len(toutes)),
        "fraicheur_des_points": dict(fraicheur), "occupation": dict(occupation.most_common(8)), "etat": dict(etat.most_common(8)), "zones": zones,
        "operateurs": [[op, n, ops_1h[op], pct(ops_1h[op], n)] for op, n in ops_total.most_common(25)],
        "exemples": exemples,
    }


def download(url):
    req = urllib.request.Request(url, headers={"User-Agent": "JUS-donnees/1.0"})
    with urllib.request.urlopen(req, timeout=300) as res:
        return res.read().decode("utf-8-sig")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="fichier CSV local (sinon téléchargement de la base officielle)")
    ap.add_argument("--source", default=SOURCE)
    ap.add_argument("--out", default="dist/data")
    ap.add_argument("--dynamic-input", help="fichier CSV local du flux de disponibilité (sinon téléchargement)")
    args = ap.parse_args()
    if args.input:
        text = Path(args.input).read_text(encoding="utf-8-sig")
        source = args.input
    else:
        req = urllib.request.Request(args.source, headers={"User-Agent": "JUS-donnees/1.0"})
        with urllib.request.urlopen(req, timeout=300) as res:
            text = res.read().decode("utf-8-sig")
        source = args.source
    sample = text[:4096]
    delim = ";" if sample.count(";") > sample.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    pdc_map = {}
    tiles, stats, diag = build(reader, pdc_map)
    if stats["stations"] < 1:
        sys.exit("Aucune station lue : format inattendu, données non publiées.")
    index = write(tiles, stats, args.out, source, diag, reader.fieldnames)
    share = 100 * (index["priced"] + index["free"]) / max(1, index["stations"])
    print("%d stations, %d tuiles, prix publié ou gratuit pour %.0f %% des stations" % (index["stations"], len(index["tiles"]), share))
    print("Sans tarif : %d, tarif présent mais illisible : %d (détail dans data/diagnostic.json)" % (diag["sans_tarif"], diag["tarif_illisible"]))
    # Carte des points de charge vers les stations, pour le serveur de disponibilité en direct
    ids, idx, pmap = [], {}, {}
    for pdc, st in pdc_map.items():
        if st["i"] not in idx:
            idx[st["i"]] = len(ids); ids.append(st["i"])
        pmap[pdc] = idx[st["i"]]
    (Path(args.out) / "pdc-map.json").write_text(json.dumps({"s": ids, "p": pmap}, separators=(",", ":")), encoding="utf-8")
    # Diagnostic de la disponibilité en direct : ne bloque jamais la publication des bornes
    out_dispo = Path(args.out) / "diagnostic-dispo.json"
    essais = []
    try:
        dyn = None
        if args.dynamic_input:
            dyn = Path(args.dynamic_input).read_text(encoding="utf-8-sig")
        else:
            for url in DYN_SOURCES:
                try:
                    dyn = download(url)
                    essais.append([url, "ok", len(dyn)])
                    break
                except Exception as e:  # noqa: BLE001
                    essais.append([url, "échec", str(e)[:200]])
        if dyn is None:
            raise RuntimeError("flux de disponibilité injoignable")
        rep = dispo_diag(dyn, pdc_map)
        rep["telechargement"] = essais
        out_dispo.write_text(json.dumps(rep, ensure_ascii=False, indent=1), encoding="utf-8")
        z = rep["zones"]["secteur_cergy"]
        print("Disponibilité en direct : %s %% des stations avec un état de moins d'1 h, %s %% de moins de 10 min (secteur Cergy : %d sur %d)"
              % (rep["pct_stations_moins_1_h"], rep["pct_stations_moins_10_min"], z["etat_moins_1_h"], z["stations"]))
    except Exception as e:  # noqa: BLE001
        print("Diagnostic de disponibilité impossible pour cette fois :", e)
        out_dispo.write_text(json.dumps({"genere": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "erreur": str(e)[:300], "telechargement": essais},
                                        ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
