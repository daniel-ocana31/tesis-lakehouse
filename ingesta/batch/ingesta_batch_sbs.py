"""
ingesta_batch_sbs.py
====================
Script de ingesta batch — Portal SBS (Empresas Financieras)
Tesis: Sistema batch-streaming Lambda/Kappa/Medallion
Autor: Daniel Arturo Ocaña Quispe — UNMSM

Descarga archivos estadísticos del portal SBS, los parsea y los
deposita en la capa Bronze (Delta Lake / Parquet local en pruebas).

Uso:
    python ingesta_batch_sbs.py                        # mes actual
    python ingesta_batch_sbs.py --anio 2025 --mes 3    # marzo 2025
    python ingesta_batch_sbs.py --catalogo             # solo listar archivos disponibles
    python ingesta_batch_sbs.py --codigos B-3208 B-3243 B-3241
"""

import os
import shutil
import argparse
import logging
import requests
import pandas as pd
import xlrd
import openpyxl
from datetime import datetime
from pathlib import Path

# ─── Configuración ───────────────────────────────────────────────────────────

BASE_URL   = "https://intranet2.sbs.gob.pe/estadistica/financiera"
BRONZE_DIR = Path("data/bronze/sbs")  # cambiar a ruta Delta Lake en producción
TEMP_DIR   = Path("data/tmp")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; tesis-lakehouse/1.0)"}

# Catálogo de archivos de Empresas Financieras (p=1)
# Identificado mediante exploración del portal SBS — Enero 2025
CATALOGO = {
    # Créditos
    "B-3208": "Creditos_Directos_Deudores_por_Tipo",
    "B-3218": "Num_Deudores_Credito_Directo",
    "B-3219": "Creditos_por_Situacion",
    "B-3220": "Creditos_por_Tipo_y_Situacion",
    "B-3221": "Creditos_Corporativos_MYPE",
    "B-3228": "Creditos_por_Departamento",
    "B-3230": "Morosidad_por_Dias_Incumplimiento",
    "B-3233": "Nuevos_Creditos_Hipotecarios",
    "B-3234": "Creditos_Castigados",
    "B-3235": "Creditos_por_Tipo_Modalidad",
    "B-3236": "Creditos_por_Modalidad",
    "B-3237": "Creditos_Indirectos",
    "B-3255": "Nuevos_Creditos_Corporativos_MYPE",
    "B-3257": "Morosidad_por_Tipo_Modalidad",
    "B-3271": "Creditos_por_Tipo_Categoria_Riesgo",
    "B-3273": "Creditos_por_Tipo_Garantia",
    # Depósitos
    "B-3211": "Movimiento_Depositos_MN",
    "B-3231": "Depositos_por_Tipo_Persona",
    "B-3232": "Num_Personas_por_Tipo_Deposito",
    "B-3238": "Depositos_por_Tipo",
    "B-3246": "Ranking_Depositos",
    "B-3251": "Depositos_Publico_MN",
    "B-3256": "Depositos_por_Escala_Montos",
    "B-3259": "Depositos_por_Departamento",
    "B-3241": "Depositos_Creditos_por_Oficina",  # archivo grande
    # Balance / Indicadores
    "B-3203": "Activos_Contingentes_Ponderados_Riesgo",
    "B-3205": "Estructura_Creditos_por_Cat_Riesgo",
    "B-3211": "Movimiento_Depositos_MN",
    "B-3222": "Estructura_Activo",
    "B-3223": "Estructura_Pasivo",
    "B-3225": "Gastos_Administracion",
    "B-3239": "Adeudos_Obligaciones",
    "B-3240": "Fideicomisos",
    "B-3250": "Ratios_Liquidez",
    "B-3252": "Patrimonio_Efectivo",
    "B-3253": "Gastos_Financieros",
    "B-3265": "Req_Patrimonio_Riesgo_Mercado",
    "B-3266": "Posicion_Global_ME",
    "B-3270": "Req_Patrimonio_Riesgo_Operacional",
    # Rankings
    "B-3243": "Ranking_Creditos_Depositos_Patrimonio",
    "B-3244": "Ranking_Creditos_por_Tipo",
    "B-3245": "Ranking_Modalidades_Credito",
    # Otros
    "B-3201": "Distribucion_Oficinas_Geografica",
    "B-3202": "Personal_por_Categoria_Laboral",
    "B-3242": "Tarjetas_Credito",
    "B-3260": "Tarjetas_Debito",
    "B-3254": "Creditos_Depositos_por_Zona",
}

MESES = {
    1: ("Enero",      "en"),
    2: ("Febrero",    "fe"),
    3: ("Marzo",      "ma"),
    4: ("Abril",      "ab"),
    5: ("Mayo",       "my"),
    6: ("Junio",      "jn"),
    7: ("Julio",      "jl"),
    8: ("Agosto",     "ag"),
    9: ("Setiembre",  "se"),
    10: ("Octubre",   "oc"),
    11: ("Noviembre", "no"),
    12: ("Diciembre", "di"),
}

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingesta_sbs")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def construir_url(codigo: str, anio: int, mes: int) -> str:
    nombre_mes, abrev_mes = MESES[mes]
    filename = f"{codigo}-{abrev_mes}{anio}.XLS"
    return f"{BASE_URL}/{anio}/{nombre_mes}/{filename}"


def descargar_archivo(codigo: str, anio: int, mes: int, dest_dir: Path) -> Path | None:
    """Descarga un archivo XLS del portal SBS. Retorna la ruta local o None si falla."""
    url = construir_url(codigo, anio, mes)
    nombre_mes, _ = MESES[mes]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{codigo}_{anio}{mes:02d}.xls"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        log.info(f"  ✅ Descargado {codigo} — {len(resp.content):,} bytes")
        return dest_path
    except requests.HTTPError as e:
        log.warning(f"  ❌ {codigo} HTTP {e.response.status_code} — {url}")
        return None
    except Exception as e:
        log.warning(f"  ❌ {codigo} ERROR: {e}")
        return None


def leer_xls(path: Path) -> pd.DataFrame | None:
    """Lee un archivo XLS (formato antiguo xlrd)."""
    try:
        wb = xlrd.open_workbook(str(path), logfile=open(os.devnull, "w"))
        ws = wb.sheet_by_index(0)
        data = [ws.row_values(r) for r in range(ws.nrows)]
        return pd.DataFrame(data)
    except Exception as e:
        log.debug(f"    xlrd falló ({e}), intentando openpyxl...")
        return None


def leer_xlsx_renombrado(path: Path) -> pd.DataFrame | None:
    """Lee un archivo que tiene extensión .xls pero es OOXML real."""
    tmp_path = path.with_suffix(".xlsx")
    wb = None
    try:
        shutil.copy(path, tmp_path)
        wb = openpyxl.load_workbook(str(tmp_path), read_only=True, data_only=True)
        ws = wb.active
        data = list(ws.iter_rows(values_only=True))
        wb.close()
        wb = None
        tmp_path.unlink(missing_ok=True)
        return pd.DataFrame(data)
    except Exception as e:
        if wb:
            wb.close()
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        log.debug(f"    openpyxl también falló: {e}")
        return None


def leer_archivo(path: Path) -> pd.DataFrame | None:
    """Intenta leer el archivo con la estrategia correcta según su contenido."""
    df = leer_xls(path)
    if df is None:
        df = leer_xlsx_renombrado(path)
    return df


def extraer_metadatos(df: pd.DataFrame) -> dict:
    """Extrae título y fecha del encabezado del archivo SBS."""
    titulo = ""
    fecha_str = ""
    for _, row in df.head(8).iterrows():
        val = str(row.iloc[0]).strip()
        if val and val not in ("None", "nan") and len(val) > 5:
            if not titulo:
                titulo = val
            # Detectar fecha (formato Excel serial o datetime)
            elif "2025" in val or "2024" in val or "2023" in val:
                fecha_str = val
                break
    return {"titulo": titulo, "fecha_reporte": fecha_str}


def guardar_bronze(df: pd.DataFrame, codigo: str, nombre: str,
                   anio: int, mes: int, bronze_dir: Path) -> Path:
    """
    Guarda el DataFrame en la capa Bronze como Parquet.
    En producción con Delta Lake: delta_table.write(..., mode='append')
    """
    particion = bronze_dir / f"anio={anio}" / f"mes={mes:02d}"
    particion.mkdir(parents=True, exist_ok=True)

    # Agregar columnas de metadata
    df = df.copy()
    df["_fuente"]   = "SBS"
    df["_codigo"]   = codigo
    df["_reporte"]  = nombre
    df["_anio"]     = anio
    df["_mes"]      = mes
    df["_ingestado"] = datetime.now().isoformat()

    # Limpiar columnas (nombres automáticos)
    df.columns = [f"col_{i}" if not isinstance(c, str) or c.strip() == ""
                  else str(c).strip() for i, c in enumerate(df.columns)]

    # Convertir todo a string (los datos SBS tienen celdas mixtas en headers)
    # En producción con PySpark se aplicará tipado en capa Silver
    for col in df.columns:
        df[col] = df[col].astype(str).replace("nan", None).replace("None", None)

    out_path = particion / f"{codigo}_{nombre}.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"  💾 Bronze guardado: {out_path} ({len(df):,} filas)")
    return out_path


# ─── Pipeline principal ───────────────────────────────────────────────────────

def ejecutar_ingesta(codigos: list[str], anio: int, mes: int,
                     bronze_dir: Path, temp_dir: Path) -> dict:
    """
    Orquesta la descarga y carga a Bronze para los códigos indicados.
    Retorna un resumen con éxitos y fallos.
    """
    nombre_mes = MESES[mes][0]
    log.info(f"🚀 Iniciando ingesta SBS — {nombre_mes} {anio}")
    log.info(f"   Archivos a procesar: {len(codigos)}")

    resumen = {"exito": [], "fallo": [], "anio": anio, "mes": mes}

    for codigo in codigos:
        nombre = CATALOGO.get(codigo, codigo)
        log.info(f"→ {codigo} — {nombre}")

        # 1. Descargar
        path_local = descargar_archivo(codigo, anio, mes, temp_dir)
        if path_local is None:
            resumen["fallo"].append(codigo)
            continue

        # 2. Leer
        df = leer_archivo(path_local)
        if df is None or df.empty:
            log.warning(f"  ⚠️  No se pudo leer {codigo}")
            resumen["fallo"].append(codigo)
            continue

        meta = extraer_metadatos(df)
        log.info(f"  📄 Título: {meta['titulo'][:60]} | {len(df)} filas × {len(df.columns)} cols")

        # 3. Guardar Bronze
        guardar_bronze(df, codigo, nombre, anio, mes, bronze_dir)
        resumen["exito"].append(codigo)

        # Limpiar temp
        path_local.unlink(missing_ok=True)

    # Resumen final
    log.info("\n" + "="*50)
    log.info(f"✅ Exitosos : {len(resumen['exito'])} — {resumen['exito']}")
    log.info(f"❌ Fallidos : {len(resumen['fallo'])} — {resumen['fallo']}")
    log.info("="*50)

    return resumen


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Ingesta batch SBS → Bronze (capa Medallion / Lambda / Kappa)"
    )
    parser.add_argument("--anio",     type=int, default=datetime.now().year,
                        help="Año del reporte (default: año actual)")
    parser.add_argument("--mes",      type=int, default=datetime.now().month,
                        help="Mes del reporte 1-12 (default: mes actual)")
    parser.add_argument("--codigos",  nargs="+", default=list(CATALOGO.keys()),
                        help="Códigos SBS a descargar (default: todos)")
    parser.add_argument("--bronze",   default=str(BRONZE_DIR),
                        help="Directorio Bronze (default: data/bronze/sbs)")
    parser.add_argument("--temp",     default=str(TEMP_DIR),
                        help="Directorio temporal (default: data/tmp)")
    parser.add_argument("--catalogo", action="store_true",
                        help="Solo mostrar catálogo y salir")
    args = parser.parse_args()

    if args.catalogo:
        print(f"\n{'Código':<12} {'Nombre'}")
        print("-" * 60)
        for cod, nom in sorted(CATALOGO.items()):
            print(f"{cod:<12} {nom}")
        print(f"\nTotal: {len(CATALOGO)} archivos")
        return

    if args.mes not in MESES:
        parser.error(f"Mes inválido: {args.mes}. Debe ser 1-12.")

    ejecutar_ingesta(
        codigos=args.codigos,
        anio=args.anio,
        mes=args.mes,
        bronze_dir=Path(args.bronze),
        temp_dir=Path(args.temp),
    )


if __name__ == "__main__":
    main()
