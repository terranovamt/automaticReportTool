import polars as pl

df = pl.DataFrame(
    {
        "TEST_TXT": [
            "vipd_GPIO_dc:pa0",
            "vipd_GPIO_dc:pa1",
            "vipu_v33_3v6_GPIO_dc:ph2",
            "vipu_v33_2v7_GPIO_dc:ph2",
            "vipu_v33_3v0_GPIO_dc:ph2",
            "vipu_v33_3v3_GPIO_dc:ph2",
            "vipu_v33_3v6_GPIO_dc:ph3",
            "vipu_v33_2v7_GPIO_dc:ph3",
            "vipu_v33_3v0_GPIO_dc:ph3",
            "vipu_v33_3v3_GPIO_dc:ph3",
            "vipu_v33_3v3_GPIO_dc",
            "vipu_v33_3v6_GPIO_dc",
            "vipu_FRC_4MHZ_GPIO_dc:ph3",
            "vipu_FRC_8MHZ_GPIO_dc:ph3",
        ]
    }
)

composite = "GPIO"
SPLIT = "(vio|vbt|v11|v12|v33|FRC)"
# Regex pattern for split cases (with split patterns)
split_pattern = rf"(.+)_({SPLIT})_([^_]+)_({composite})_([^:]+)(?::(.+))?"
std_pattern = rf"(.*)_({composite})_([^:]+)(?::(.+))?"

# Creare una colonna binaria temporanea per identificare i casi split
df = df.with_columns(pl.col("TEST_TXT").str.contains(split_pattern).alias("_is_split"))

# Processare i dati
df = df.with_columns(
    [
        # TEST_TXT: Ricostruire per i casi split, mantenere originale per STD
        pl.when(pl.col("_is_split"))
        .then(
            pl.when(pl.col("TEST_TXT").str.extract(split_pattern, 7).is_not_null())
            .then(
                # Caso con gruppo 7 presente: include il gruppo 7
                pl.concat_str(
                    [
                        pl.col("TEST_TXT").str.extract(
                            split_pattern, 1
                        ),  # prefix (vipu)
                        pl.lit("_"),
                        pl.col("TEST_TXT").str.extract(
                            split_pattern, 5
                        ),  # suffix (dc:ph2)
                        pl.lit(":"),
                        pl.col("TEST_TXT").str.extract(
                            split_pattern, 7
                        ),  # suffix (dc:ph2)
                    ]
                )
            )
            .otherwise(
                # Caso senza gruppo 7: solo prefix + gruppo 5
                pl.concat_str(
                    [
                        pl.col("TEST_TXT").str.extract(
                            split_pattern, 1
                        ),  # prefix (vipu)
                        pl.lit("_"),
                        pl.col("TEST_TXT").str.extract(
                            split_pattern, 5
                        ),  # suffix (dc:ph2)
                    ]
                )
            )
        )
        .otherwise(
            pl.when(pl.col("TEST_TXT").str.extract(std_pattern, 4).is_not_null())
            .then(
                pl.concat_str(
                    [
                        pl.col("TEST_TXT").str.extract(std_pattern, 1),
                        pl.lit("_"),
                        pl.col("TEST_TXT").str.extract(std_pattern, 2),
                        pl.lit(":"),
                        pl.col("TEST_TXT").str.extract(std_pattern, 4),
                    ]
                )
            )
            .otherwise(
                # Caso senza gruppo 7: solo prefix + gruppo 5
                pl.concat_str(
                    [
                        pl.col("TEST_TXT").str.extract(std_pattern, 1),  # prefix (vipu)
                        pl.lit("_"),
                        pl.col("TEST_TXT").str.extract(
                            std_pattern, 2
                        ),  # suffix (dc:ph2)
                    ]
                )
            )
        )
        .alias("TEST_TXT"),
        # SPLIT: Estrarre il valore split o "standard"
        pl.when(pl.col("_is_split"))
        .then(
            pl.col("TEST_TXT").str.extract(
                split_pattern, 3
            )  # split_value (3v6, 2v7, FRC, etc.)
        )
        .otherwise(pl.lit("standard"))
        .alias("Split"),
        # pltype: "SPLIT" o "STD"
        pl.when(pl.col("_is_split"))
        .then(pl.lit("SPLIT"))
        .otherwise(pl.lit("STD"))
        .alias("pltype"),
    ]
).drop(
    "_is_split"
)  # Rimuovere la colonna binaria temporanea

print(df)


df = pl.DataFrame(
    {
        "TEST_TXT": [
            "vipd_GPIO_dc:pa0",
            "vipd_GPIO_dc:pa1",
            "vipu_v33_3v6_GPIO_dc:ph2",
            "vipu_v33_2v7_GPIO_dc:ph2",
            "vipu_v33_3v0_GPIO_dc:ph2",
            "vipu_v33_3v3_GPIO_dc:ph2",
            "vipu_v33_3v6_GPIO_dc:ph3",
            "vipu_v33_2v7_GPIO_dc:ph3",
            "vipu_v33_3v0_GPIO_dc:ph3",
            "vipu_v33_3v3_GPIO_dc:ph3",
            "vipu_FRC_4MHZ_GPIO_dc:ph3",
            "vipu_FRC_8MHZ_GPIO_dc:ph3",
        ]
    }
)

# Pattern per i casi split: cattura prefix, split_type, split_value, composite, suffix
split_pattern = rf"(.+)_({SPLIT})_([^_]+)_({composite})_(.*)"

# Creare una colonna binaria temporanea per identificare i casi split
df = df.with_columns(pl.col("TEST_TXT").str.contains(split_pattern).alias("_is_split"))

# Salvare il TEST_TXT originale per l'estrazione
df = df.with_columns(pl.col("TEST_TXT").alias("_original_txt"))

# Processare i dati
df = df.with_columns(
    [
        # TEST_TXT: Ricostruire per i casi split, mantenere originale per STD
        pl.when(pl.col("_is_split"))
        .then(
            pl.concat_str(
                [
                    pl.col("_original_txt").str.extract(
                        split_pattern, 1
                    ),  # prefix (vipu)
                    pl.lit("_"),
                    pl.col("_original_txt").str.extract(
                        split_pattern, 4
                    ),  # composite (GPIO)
                    pl.lit("_"),
                    pl.col("_original_txt").str.extract(
                        split_pattern, 5
                    ),  # suffix (dc:ph2)
                ]
            )
        )
        .otherwise(
            [
                pl.col("TEST_TXT").str.extract(split_pattern, 1),
                pl.lit("_"),
                pl.col("TEST_TXT").str.extract(split_pattern, 6),
            ]
        )
        .alias("TEST_TXT"),
        # SPLIT: Estrarre il valore split o "standard"
        pl.when(pl.col("_is_split"))
        .then(
            pl.col("_original_txt").str.extract(split_pattern, 3)
        )  # split_value (3v6, 2v7, 4MHZ, 8MHZ)
        .otherwise(pl.lit("standard"))
        .alias("SPLIT"),
        # pltype: "SPLIT" o "STD"
        pl.when(pl.col("_is_split"))
        .then(pl.lit("SPLIT"))
        .otherwise(pl.lit("STD"))
        .alias("pltype"),
    ]
).drop(
    ["_is_split", "_original_txt"]
)  # Rimuovere le colonne temporanee

print(df)


# qwusto codice no nfunziona alla fine vorrei la dataframe cosi
# SPLIT e; corretto perche poi nqeusto case ne hai solo acun ii naltri cas ipotresi averne altri
# non usare colonne aggiuntive se non strettametne necessatio al massimo usa colonne binarei per non occupare memoria e cancellale quando non ti servono piu

# TEST_TXT, SPLIT, pltype
# vipd_GPIO_dc:pa0, standard,STD
# vipd_GPIO_dc:pa1, standard,STD
# vipu_GPIO_dc:ph2, 3v6,SPLIT
# vipu_GPIO_dc:ph2, 2v7,SPLIT
# vipu_GPIO_dc:ph2, 3v0,SPLIT
# vipu_GPIO_dc:ph2, 3v3,SPLIT
# vipu_GPIO_dc:ph3, 3v6,SPLIT
# vipu_GPIO_dc:ph3, 2v7,SPLIT
# vipu_GPIO_dc:ph3, 3v0,SPLIT
# vipu_GPIO_dc:ph3, 3v3,SPLIT
