def gen_actions():
    with open(CSV_PATH, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc = {
                "row_id": row["row_id"],
                "proximidad_volcan_km": float(row["proximidad_volcan_km"]) if row["proximidad_volcan_km"] else None,
                "exposicion_alta": int(row["exposicion_alta"]) if row["exposicion_alta"] else None,
                "jefatura_femenina": int(row["jefatura_femenina"]) if row["jefatura_femenina"] else None,
                "adultos_mayores": int(row["adultos_mayores"]) if row["adultos_mayores"] else None,
                "vulnerabilidad_score": int(row["vulnerabilidad_score"]) if row["vulnerabilidad_score"] else None,
            }
            yield {"_op_type": "index", "_index": INDEX, "_source": doc}
