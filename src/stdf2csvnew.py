import sys, os
from pystdf.Importer import STDF2DataFrame
import pystdf.V4 as V4
import pandas as pd


def toExcel(fname, tables):
    """Esporta le tabelle da toTables a CSV"""
    for k, v in tables.items():
        # Assicurati che l'ordine delle colonne rispetti le specifiche
        record = [r for r in V4.records if r.__class__.__name__.upper() == k]

        if len(record) == 0:
            print(
                "Ignora l'esportazione della tabella %s: Nessun tipo di record esistente."
                % k
            )
        else:
            columns = [field[0] for field in record[0].fieldMap]

            # Controlla se tutte le colonne sono presenti nel DataFrame
            missing_columns = [col for col in columns if col not in v.columns]
            if missing_columns:
                print(
                    f"Attenzione: Le seguenti colonne mancano e saranno ignorate: {missing_columns}"
                )
                columns = [col for col in columns if col in v.columns]

            # Esporta il DataFrame in un file CSV
            csv_fname = f"{fname}_{k}.csv"
            v.to_csv(csv_fname, columns=columns, index=False, na_rep="N/A")
            print(f"Esportato {k} a {csv_fname}")


def stdf2csv_converter(fin,fout):
    print("Importing %s" % fin)
    dfs = STDF2DataFrame(fin)
    print("Exporting to %s" % fout)
    toExcel(fout, dfs)

    # if len(sys.argv) == 1:
def main():
    #     print("Usage: %s <stdf file>" % (sys.argv[0]))
    # else:
    #     fin = sys.argv[1]
    #     if len(sys.argv) > 2:
    #         fout = sys.argv[2]
    #     else:
    #         fout = fin[: fin.rfind(".")]
    #     print("Importing %s" % fin)
    #     dfs = STDF2DataFrame(fin)
    #     print("Exporting to %s" % fout)
    #     toExcel(fout, dfs)

    fin = ".\\test1.std"
    fout = fin[: fin.rfind(".")]
    print("Importing %s" % fin)
    dfs = STDF2DataFrame(fin)
    print("Exporting to %s" % fout)
    toExcel(fout, dfs)

    # fin = ".\\test2.std"
    # fout = fin[: fin.rfind(".")]
    # print("Importing %s" % fin)
    # dfs = STDF2DataFrame(fin)
    # print("Exporting to %s" % fout)
    # toExcel(fout, dfs)

    # fin = ".\\test3.stdF"
    # fout = fin[: fin.rfind(".")]
    # print("Importing %s" % fin)
    # dfs = STDF2DataFrame(fin)
    # print("Exporting to %s" % fout)
    # toExcel(fout, dfs)

    # fin = ".\\example0.std"
    # fout = fin[: fin.rfind(".")]
    # print("Importing %s" % fin)
    # dfs = STDF2DataFrame(fin)
    # print("Exporting to %s" % fout)
    # toExcel(fout, dfs)

    fin = ".\\44EFT.std"
    fout = fin[: fin.rfind(".")]
    print("Importing %s" % fin)
    dfs = STDF2DataFrame(fin)
    print("Exporting to %s" % fout)
    toExcel(fout, dfs)

    fin = ".\\443616_12.std"
    fout = fin[: fin.rfind(".")]
    print("Importing %s" % fin)
    dfs = STDF2DataFrame(fin)
    print("Exporting to %s" % fout)
    toExcel(fout, dfs)

    fin = ".\\sample.std"
    fout = fin[: fin.rfind(".")]
    print("Importing %s" % fin)
    dfs = STDF2DataFrame(fin)
    print("Exporting to %s" % fout)
    toExcel(fout, dfs)

if __name__ == "__main__":
    main()
