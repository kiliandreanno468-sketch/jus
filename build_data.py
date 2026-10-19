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
CELL = 0.25  # taille d'une tuile, en degrés
FRANCE = (41.0, 51.6, -5.5, 10.0)  # lat min, lat max, lon min, lon max (métropole et Corse)

NUM = r"(\d+(?:[.,]\d+)?)"
EUR = r"\s*(?:€|eur(?:os?)?)\s*(?:ttc\s*)?"


def fr_num(s):
    return float(s.replace(",", "."))


def parse_tarif(text):
    """Extrait €/kWh, €/min (et délai), frais de session et gratuité d'un texte libre."""
    out = {"kwh": None, "min": None, "after": None, "ses": None, "free": False}
    if not text:
        return out
    t = str(text).replace("\u00a0", " ")
    if re.match(r"^\s*gratuit", t, re.I):
        out["free"] = True
    m = re.search(NUM + EUR + r"(?:/|par|le)\s*kwh", t, re.I) or re.search(NUM + r"\s*(?:ttc\s*)?/\s*kwh", t, re.I)
    if m:
        k = fr_num(m.group(1))
        if 0.03 < k < 2:
            out["kwh"] = k
    m = re.search(NUM + EUR + r"(?:/|par|la)\s*min", t, re.I)
    if m:
        p = fr_num(m.group(1))
        if 0 < p < 2:
            out["min"] = p
    if out["min"] is None:
        m = re.search(NUM + EUR + r"(?:/|par|l')\s*h(?:eure)?\b", t, re.I)
        if m:
            h = fr_num(m.group(1))
            if 0 < h < 60:
                out["min"] = round(h / 60, 3)
    if out["min"] is not None:
        m = re.search(r"(?:après|apres|au[- ]del[aà] de|au-delà de)\s*(\d+)\s*(min|h)", t, re.I)
        if m:
            n = int(m.group(1))
            out["after"] = n * 60 if m.group(2).lower() == "h" else n
    m = re.search(NUM + EUR + r"(?:/|par|la)\s*(?:session|charge|recharge|connexion)", t, re.I) or \
        re.search(r"frais (?:de )?(?:session|connexion|démarrage|demarrage)\s*:?\s*" + NUM, t, re.I)
    if m:
        s = fr_num(m.group(1))
        if 0 < s < 20:
            out["ses"] = s
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


def cell_key(lat, lon):
    return "%.2f_%.2f" % (math.floor(lat / CELL) * CELL, math.floor(lon / CELL) * CELL)


def build(rows):
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
        key = row.get("id_station_itinerance") or "%s|%.5f|%.5f" % (row.get("nom_station", ""), lat, lon)
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="fichier CSV local (sinon téléchargement de la base officielle)")
    ap.add_argument("--source", default=SOURCE)
    ap.add_argument("--out", default="dist/data")
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
    tiles, stats, diag = build(reader)
    if stats["stations"] < 1:
        sys.exit("Aucune station lue : format inattendu, données non publiées.")
    index = write(tiles, stats, args.out, source, diag, reader.fieldnames)
    share = 100 * (index["priced"] + index["free"]) / max(1, index["stations"])
    print("%d stations, %d tuiles, prix publié ou gratuit pour %.0f %% des stations" % (index["stations"], len(index["tiles"]), share))
    print("Sans tarif : %d, tarif présent mais illisible : %d (détail dans data/diagnostic.json)" % (diag["sans_tarif"], diag["tarif_illisible"]))


if __name__ == "__main__":
    main()
