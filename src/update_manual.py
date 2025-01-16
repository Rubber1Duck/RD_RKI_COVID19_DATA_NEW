import os
import datetime as dt
import time
import numpy as np
import pandas as pd
import json
import utils as ut
from shutil import copy
import fallzahlen_update
import changes_history
from multiprocess_pandas import applyparallel

if __name__ == "__main__":
    startTime = dt.datetime.now()
    repo_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    meta_path = os.path.join(repo_path, "dataStore", "meta")
    filename_meta = "meta_new.json"

    BV_csv_path = os.path.join(repo_path, "Bevoelkerung", "Bevoelkerung.csv")
    LK_dtypes = {
        "Datenstand": "object",
        "IdLandkreis": "str",
        "Landkreis": "str",
        "incidence_7d": "float64",
    }
    LK_dtypes_single_files = {
        "Datenstand": "object",
        "IdLandkreis": "str",
        "Landkreis": "str",
        "AnzahlFall_7d": "int32",
        "incidence_7d": "float64",
    }
    BL_dtypes = {
        "Datenstand": "object",
        "IdBundesland": "str",
        "Bundesland": "str",
        "incidence_7d": "float64",
    }
    kum_dtypes = {"D": "object", "I": "str", "T": "str", "i": "float64"}
    BV_dtypes = {
        "AGS": "str",
        "Altersgruppe": "str",
        "Name": "str",
        "GueltigAb": "object",
        "GueltigBis": "object",
        "Einwohner": "Int32",
        "männlich": "Int32",
        "weiblich": "Int32",
    }
    CV_dtypes = {
        "IdLandkreis": "str",
        "Altersgruppe": "str",
        "Geschlecht": "str",
        "NeuerFall": "Int32",
        "NeuerTodesfall": "Int32",
        "NeuGenesen": "Int32",
        "AnzahlFall": "Int32",
        "AnzahlTodesfall": "Int32",
        "AnzahlGenesen": "Int32",
        "Meldedatum": "object",
    }

    # open bevoelkerung.csv
    BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
    BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

    BV_BL = BV[BV["AGS"].str.len() == 2].copy()
    BV_BL.reset_index(inplace=True, drop=True)

    BV_BL_A00 = BV_BL[BV_BL["Altersgruppe"] == "A00+"].copy()
    BV_BL_A00.reset_index(inplace=True, drop=True)

    BV_LK = BV[BV["AGS"].str.len() == 5].copy()
    BV_LK.reset_index(inplace=True, drop=True)

    BV_LK_A00 = BV_LK[BV_LK["Altersgruppe"] == "A00+"].copy()
    BV_LK_A00.reset_index(inplace=True, drop=True)

    # load covid latest from web
    t1 = time.time()
    with open(meta_path + "/" + filename_meta, "r", encoding="utf8") as file:
        metaObj = json.load(file)
    fileNameOrig = metaObj["filename"]
    fileSize = int(metaObj["size"])
    url = metaObj["url"]
    urlpath = os.path.dirname(url)
    timeStamp = metaObj["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    filedate = (
        dt.datetime.fromtimestamp(metaObj["modified"] / 1000)
        .date()
        .strftime("%Y-%m-%d")
    )
    fileyear = (
        dt.datetime.fromtimestamp(metaObj["modified"] / 1000).date().strftime("%Y")
    )

    fileSizeMb = round(fileSize / 1024 / 1024, 1)
    fileNameRoot = "RKI_COVID19_"
    fileName = fileNameRoot + filedate + ".csv"
    fileNameFeather = fileNameRoot + filedate + ".feather"
    featherfull = os.path.join(urlpath, fileNameFeather)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")

    # use feather file is exists, if not use *.csv.xz and create featherfile
    if os.path.exists(featherfull):
        print(f"{aktuelleZeit} : load {fileNameOrig} with feather file ...", end="")
        LK = ut.read_file(featherfull)
    else:
        print(
            f"{aktuelleZeit} : load {fileNameOrig} (size: {fileSize} bytes => {fileSizeMb} MegaByte)  ...",
            end="",
        )
        LK = pd.read_csv(
            url, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes
        )
        # ----- Squeeze the dataframe
        LK = ut.squeeze_dataframe(LK)
        ut.write_file(df=LK, fn=fileNameFeather, compression="lz4")

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" {LK.shape[0]} rows. done in {round((t2 - t1), 3)} secs.")
    t1 = time.time()
    data_path = os.path.normpath(os.path.join(repo_path, "data", fileyear))
    os.makedirs(data_path, exist_ok=True)
    fileNameXz = fileName + ".xz"
    full_path = os.path.join(data_path, fileName)
    full_pathXz = os.path.join(data_path, fileNameXz)
    istDatei = os.path.isfile(full_path)
    istDateiXz = os.path.isfile(full_pathXz)
    if not (istDatei | istDateiXz):
        print(f"{aktuelleZeit} : copy source Data to data ...", end="")
        copy(src=url, dst=full_pathXz)
        t2 = time.time()
        print(f" done in {round((t2 - t1), 3)} secs.")
    else:
        if istDatei:
            fileExists = fileName
        else:
            fileExists = fileNameXz
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(f"{aktuelleZeit} : {fileExists} already exists.")

    print(f"{aktuelleZeit} : add missing columns ...", end="")
    t1 = time.time()
    LK["IdLandkreis"] = LK["IdLandkreis"].map("{:0>5}".format)
    LK.insert(loc=0, column="IdBundesland", value=LK["IdLandkreis"].str.slice(0, 2))
    LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    LK.insert(loc=0, column="Datenstand", value=Datenstand.date())

    # ----- Squeeze the dataframe
    LK = ut.squeeze_dataframe(LK)
    feather_path = os.path.join(repo_path, fileNameRoot + filedate + ".feather")
    # store dataBase to feather file to save memory
    ut.write_file(df=LK, fn=feather_path, compression="lz4")
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")

    # ageGroup Data erst ab 2021-08-26! vorher gibt es keine Bevölkerungszahlen getrennt nach Geschlecht
    if Datenstand >= dt.datetime.strptime("2021-08-26", "%Y-%m-%d"):
        print(f"{aktuelleZeit} : calculating age-group data ...", end="")
        t1 = time.time()
        # kopiere dataBase ohne unbekannte Altersgruppen oder unbekannte Geschlechter
        LK = LK[LK["Altersgruppe"] != "unbekannt"].copy()
        LK = LK[LK["Geschlecht"] != "unbekannt"].copy()

        # korrigiere Kategorien Altersgruppe und Geschlecht
        LK["Geschlecht"] = LK["Geschlecht"].cat.remove_unused_categories()
        LK["Altersgruppe"] = LK["Altersgruppe"].cat.remove_unused_categories()

        # lösche alle nicht benötigten Spalten
        LK.drop(["Meldedatum", "Datenstand"], inplace=True, axis=1)

        # used keylists
        key_list_LK_age = ["IdLandkreis", "Altersgruppe"]
        key_list_BL_age = ["IdBundesland", "Altersgruppe"]
        key_list_ID0_age = ["Altersgruppe"]

        # calculate the age group data
        LK["AnzahlFall"] = np.where(
            LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
        ).astype(int)
        LK["casesMale"] = np.where(LK["Geschlecht"] == "M", LK["AnzahlFall"], 0).astype(
            int
        )
        LK["casesFemale"] = np.where(
            LK["Geschlecht"] == "W", LK["AnzahlFall"], 0
        ).astype(int)
        LK["AnzahlTodesfall"] = np.where(
            LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
        ).astype(int)
        LK["deathsMale"] = np.where(
            LK["Geschlecht"] == "M", LK["AnzahlTodesfall"], 0
        ).astype(int)
        LK["deathsFemale"] = np.where(
            LK["Geschlecht"] == "W", LK["AnzahlTodesfall"], 0
        ).astype(int)
        LK.drop(
            [
                "NeuGenesen",
                "NeuerFall",
                "NeuerTodesfall",
                "AnzahlFall",
                "AnzahlTodesfall",
                "AnzahlGenesen",
                "Geschlecht",
            ],
            inplace=True,
            axis=1,
        )
        agg_key = {
            c: "max" if c in ["IdBundesland"] else "sum"
            for c in LK.columns
            if c not in key_list_LK_age
        }
        LK = LK.groupby(by=key_list_LK_age, as_index=False, observed=True).agg(agg_key)
        BV_mask = (
            (BV["AGS"].isin(LK["IdLandkreis"]))
            & (BV["Altersgruppe"].isin(LK["Altersgruppe"]))
            & (BV["GueltigAb"] <= Datenstand)
            & (BV["GueltigBis"] >= Datenstand)
        )
        BV_masked = BV[BV_mask].copy()
        BV_masked.drop(["GueltigAb", "GueltigBis", "Einwohner"], inplace=True, axis=1)
        BV_masked.rename(columns={"AGS": "IdLandkreis"}, inplace=True)

        LK = LK.merge(right=BV_masked, on=["IdLandkreis", "Altersgruppe"], how="left")
        LK = LK[LK["Name"].notna()]
        LK.drop(["Name"], inplace=True, axis=1)

        LK["casesMale"] = LK["casesMale"].astype(int)
        LK["casesFemale"] = LK["casesFemale"].astype(int)
        LK["deathsMale"] = LK["deathsMale"].astype(int)
        LK["deathsFemale"] = LK["deathsFemale"].astype(int)
        LK["casesMalePer100k"] = round(LK["casesMale"] / LK["männlich"] * 100000, 1)
        LK["casesFemalePer100k"] = round(LK["casesFemale"] / LK["weiblich"] * 100000, 1)
        LK["deathsMalePer100k"] = round(LK["deathsMale"] / LK["männlich"] * 100000, 1)
        LK["deathsFemalePer100k"] = round(
            LK["deathsFemale"] / LK["weiblich"] * 100000, 1
        )

        agg_key = {
            c: "max" if c in ["IdLandkreis"] else "sum"
            for c in LK.columns
            if c not in key_list_BL_age
        }
        BL = (
            LK.groupby(by=key_list_BL_age, as_index=False, observed=True)
            .agg(agg_key)
            .copy()
        )
        BL["casesMalePer100k"] = round(BL["casesMale"] / BL["männlich"] * 100000, 1)
        BL["casesFemalePer100k"] = round(BL["casesFemale"] / BL["weiblich"] * 100000, 1)
        BL["deathsMalePer100k"] = round(BL["deathsMale"] / BL["männlich"] * 100000, 1)
        BL["deathsFemalePer100k"] = round(
            BL["deathsFemale"] / BL["weiblich"] * 100000, 1
        )
        BL.drop(["IdLandkreis"], inplace=True, axis=1)
        agg_key = {
            c: "max" if c in ["IdBundesland", "IdLandkreis"] else "sum"
            for c in BL.columns
            if c not in key_list_ID0_age
        }
        ID0 = (
            BL.groupby(by=key_list_ID0_age, as_index=False, observed=False)
            .agg(agg_key)
            .copy()
        )
        ID0["IdBundesland"] = "00"
        ID0["casesMalePer100k"] = round(ID0["casesMale"] / ID0["männlich"] * 100000, 1)
        ID0["casesFemalePer100k"] = round(
            ID0["casesFemale"] / ID0["weiblich"] * 100000, 1
        )
        ID0["deathsMalePer100k"] = round(
            ID0["deathsMale"] / ID0["männlich"] * 100000, 1
        )
        ID0["deathsFemalePer100k"] = round(
            ID0["deathsFemale"] / ID0["weiblich"] * 100000, 1
        )
        LK.drop(["männlich", "weiblich", "IdBundesland"], inplace=True, axis=1)
        BL.drop(["männlich", "weiblich"], inplace=True, axis=1)
        ID0.drop(["männlich", "weiblich"], inplace=True, axis=1)

        BL = pd.concat([ID0, BL])
        BL.reset_index(inplace=True, drop=True)

        # store as xz compresed json
        path = os.path.join(repo_path, "dataStore", "agegroup")
        os.makedirs(path, exist_ok=True)
        ut.write_json(LK, os.path.join(path, "districts.json"))
        ut.write_json(BL, os.path.join(path, "states.json"))
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        t2 = time.time()
        print(f" done in {round((t2 - t1), 3)} secs.")

    # accumulated and new cases, deaths, recovered, casesPerWeek, deathsPerWeek
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating new and accumulated data ...", end="")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)

    key_list_ID0_cases = ["IdStaat"]

    # calculate the values
    LK["accuCases"] = np.where(
        LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["newCases"] = np.where(
        LK["NeuerFall"].isin([1, -1]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["accuCasesPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["accuCases"],
        0,
    ).astype(int)
    LK["newCasesPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["newCases"], 0
    ).astype(int)
    LK["accuDeaths"] = np.where(
        LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["newDeaths"] = np.where(
        LK["NeuerTodesfall"].isin([1, -1]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["accuDeathsPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["accuDeaths"],
        0,
    ).astype(int)
    LK["newDeathsPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["newDeaths"],
        0,
    ).astype(int)
    LK["accuRecovered"] = np.where(
        LK["NeuGenesen"].isin([1, 0]), LK["AnzahlGenesen"], 0
    ).astype(int)
    LK["newRecovered"] = np.where(
        LK["NeuGenesen"].isin([1, -1]), LK["AnzahlGenesen"], 0
    ).astype(int)
    LK.drop(
        [
            "NeuGenesen",
            "NeuerFall",
            "NeuerTodesfall",
            "AnzahlFall",
            "AnzahlTodesfall",
            "AnzahlGenesen",
            "Altersgruppe",
            "Geschlecht",
        ],
        inplace=True,
        axis=1,
    )
    agg_key = {
        "Datenstand": "max",
        "IdBundesland": "max",
        "Meldedatum": "max",
        "accuCases": "sum",
        "newCases": "sum",
        "accuCasesPerWeek": "sum",
        "newCasesPerWeek": "sum",
        "accuDeaths": "sum",
        "newDeaths": "sum",
        "accuDeathsPerWeek": "sum",
        "newDeathsPerWeek": "sum",
        "accuRecovered": "sum",
        "newRecovered": "sum",
    }

    LK = LK.groupby(by=["IdLandkreis"], as_index=False, observed=False).agg(agg_key)
    agg_key = {
        "IdLandkreis": "max",
        "Datenstand": "max",
        "Meldedatum": "max",
        "accuCases": "sum",
        "newCases": "sum",
        "accuCasesPerWeek": "sum",
        "newCasesPerWeek": "sum",
        "accuDeaths": "sum",
        "newDeaths": "sum",
        "accuDeathsPerWeek": "sum",
        "newDeathsPerWeek": "sum",
        "accuRecovered": "sum",
        "newRecovered": "sum",
    }

    BL = LK.groupby(by=["IdBundesland"], as_index=False, observed=False).agg(agg_key)
    BL.insert(loc=0, column="IdStaat", value="00")
    agg_key = {
        "IdBundesland": "max",
        "IdLandkreis": "max",
        "Datenstand": "max",
        "Meldedatum": "max",
        "accuCases": "sum",
        "newCases": "sum",
        "accuCasesPerWeek": "sum",
        "newCasesPerWeek": "sum",
        "accuDeaths": "sum",
        "newDeaths": "sum",
        "accuDeathsPerWeek": "sum",
        "newDeathsPerWeek": "sum",
        "accuRecovered": "sum",
        "newRecovered": "sum",
    }

    ID0 = BL.groupby(by=["IdStaat"], as_index=False, observed=False).agg(agg_key)

    BL.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)

    mask = (
        (BV["AGS"].isin(LK["IdLandkreis"]))
        & (BV["Altersgruppe"] == "A00+")
        & (BV["GueltigAb"] <= Datenstand)
        & (BV["GueltigBis"] >= Datenstand)
    )
    masked = BV[mask].copy()
    masked.drop(
        ["GueltigAb", "GueltigBis", "männlich", "weiblich", "Altersgruppe"],
        inplace=True,
        axis=1,
    )
    masked.rename(
        columns={"AGS": "IdLandkreis", "Einwohner": "population", "Name": "Landkreis"},
        inplace=True,
    )
    LK = LK.merge(right=masked, on="IdLandkreis", how="left")
    mask = (
        (BV["AGS"].isin(BL["IdBundesland"]))
        & (BV["Altersgruppe"] == "A00+")
        & (BV["GueltigAb"] <= Datenstand)
        & (BV["GueltigBis"] >= Datenstand)
    )
    masked = BV[mask].copy()
    masked.drop(
        ["GueltigAb", "GueltigBis", "männlich", "weiblich", "Altersgruppe"],
        inplace=True,
        axis=1,
    )
    masked.rename(
        columns={
            "AGS": "IdBundesland",
            "Einwohner": "population",
            "Name": "Bundesland",
        },
        inplace=True,
    )
    BL = BL.merge(right=masked, on="IdBundesland", how="left")
    masked.drop(["population"], inplace=True, axis=1)
    LK = LK.merge(right=masked, on="IdBundesland", how="left")
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK["Datenstand"] = LK["Datenstand"].astype(str)
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    BL["Datenstand"] = BL["Datenstand"].astype(str)
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    # store as gz compressed json
    path = os.path.normpath(os.path.join(repo_path, "dataStore", "cases"))
    os.makedirs(path, exist_ok=True)
    ut.write_json(LK, os.path.join(path, "districts.json"))
    ut.write_json(BL, os.path.join(path, "states.json"))
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")

    # History
    # DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
    # StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
    print(f"{aktuelleZeit} : calculating history data ...")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)

    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["AnzahlTodesfall"] = np.where(
        LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["AnzahlGenesen"] = np.where(
        LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0
    ).astype(int)
    LK.drop(
        [
            "NeuerFall",
            "NeuerTodesfall",
            "NeuGenesen",
            "Altersgruppe",
            "Geschlecht",
            "Datenstand",
        ],
        inplace=True,
        axis=1,
    )
    LK.rename(
        columns={
            "AnzahlFall": "cases",
            "AnzahlTodesfall": "deaths",
            "AnzahlGenesen": "recovered",
        },
        inplace=True,
    )
    agg_key = {
        "IdBundesland": "max",
        "cases": "sum",
        "deaths": "sum",
        "recovered": "sum",
    }
    LK = LK.groupby(
        by=["IdLandkreis", "Meldedatum"], as_index=False, observed=True
    ).agg(agg_key)
    agg_key = {
        "IdLandkreis": "max",
        "cases": "sum",
        "deaths": "sum",
        "recovered": "sum",
    }
    BL = LK.groupby(
        by=["IdBundesland", "Meldedatum"], as_index=False, observed=True
    ).agg(agg_key)
    agg_key = {
        "IdBundesland": "max",
        "IdLandkreis": "max",
        "cases": "sum",
        "deaths": "sum",
        "recovered": "sum",
    }
    ID0 = BL.groupby(by=["Meldedatum"], as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK.sort_values(by=["IdLandkreis", "Meldedatum"], inplace=True)
    LK.reset_index(inplace=True, drop=True)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.sort_values(by=["IdBundesland", "Meldedatum"], inplace=True)
    BL.reset_index(inplace=True, drop=True)

    # fill dates for every region
    allDates = pd.DataFrame(
        pd.date_range(
            end=(Datenstand - dt.timedelta(days=1)), start="2019-12-26"
        ).strftime("%Y-%m-%d"),
        columns=["Meldedatum"],
    )
    BLDates = pd.DataFrame(
        pd.unique(BL["IdBundesland"]).copy(), columns=["IdBundesland"]
    )
    LKDates = pd.DataFrame(pd.unique(LK["IdLandkreis"]).copy(), columns=["IdLandkreis"])
    # add Bundesland, Landkreis and Einwohner
    BV_mask = (
        (BV_BL_A00["AGS"].isin(BLDates["IdBundesland"]))
        & (BV_BL_A00["GueltigAb"] <= Datenstand)
        & (BV_BL_A00["GueltigBis"] >= Datenstand)
    )
    BV_masked = BV_BL_A00[BV_mask].copy()
    BV_masked.drop(
        ["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BV_masked.rename(
        columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True
    )
    BLDates = BLDates.merge(right=BV_masked, on=["IdBundesland"], how="left")

    BV_mask = (
        (BV_LK_A00["AGS"].isin(LK["IdLandkreis"]))
        & (BV_LK_A00["GueltigAb"] <= Datenstand)
        & (BV_LK_A00["GueltigBis"] >= Datenstand)
    )
    BV_masked = BV_LK_A00[BV_mask].copy()
    BV_masked.drop(
        ["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BV_masked.rename(columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True)
    LKDates = LKDates.merge(right=BV_masked, on="IdLandkreis", how="left")

    BLDates = BLDates.merge(allDates, how="cross")
    BLDates = ut.squeeze_dataframe(BLDates)
    LKDates = LKDates.merge(allDates, how="cross")
    LKDates = ut.squeeze_dataframe(LKDates)
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    BL = BL.merge(BLDates, how="right", on=["IdBundesland", "Meldedatum"])
    LK = LK.merge(LKDates, how="right", on=["IdLandkreis", "Meldedatum"])

    # fill nan with 0
    BL["cases"] = BL["cases"].fillna(0).astype(int)
    BL["deaths"] = BL["deaths"].fillna(0).astype(int)
    BL["recovered"] = BL["recovered"].fillna(0).astype(int)

    LK["cases"] = LK["cases"].fillna(0).astype(int)
    LK["deaths"] = LK["deaths"].fillna(0).astype(int)
    LK["recovered"] = LK["recovered"].fillna(0).astype(int)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        f"{aktuelleZeit} :   |-calculating BL incidence {BL.shape[0]} rows ...", end=""
    )
    t11 = time.time()
    BL = BL.groupby(["IdBundesland"], observed=True).apply_parallel(ut.calc_incidence)
    t12 = time.time()
    print(f" Done in {round(t12 - t11, 3)} sec.")
    BL.reset_index(inplace=True, drop=True)
    BL["incidence7d"] = (BL["cases7d"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        f"{aktuelleZeit} :   |-calculating LK incidence {LK.shape[0]} rows ...", end=""
    )
    t11 = time.time()
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence)
    t12 = time.time()
    print(f" Done in {round(t12-t11, 3)} sec.")
    LK.reset_index(inplace=True, drop=True)
    LK["incidence7d"] = (LK["cases7d"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(
        f"{aktuelleZeit} : history data calculation done in {round((t2 - t1), 3)} secs."
    )

    print(f"{aktuelleZeit} : write files to disk ...", end="")
    t1 = time.time()
    # store json
    path = os.path.join(repo_path, "dataStore", "history")
    os.makedirs(path, exist_ok=True)
    # complete districts (cases, deaths, recovered. incidence)
    ut.write_json(LK, os.path.join(path, "districts.json"))
    # complete states (cases, deaths, recovered. incidence)
    ut.write_json(BL, os.path.join(path, "states.json"))
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round(t2 - t1, 3)} secs.")

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating change history ...", end="")
    t1 = time.time()
    path = os.path.join(repo_path, "dataStore", "historychanges")
    os.makedirs(path, exist_ok=True)
    
    # *******************
    # * changes history *
    # *******************

    changes_history.update(LK, BL, Datenstand, repo_path)

    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f" done in {round((t2 - t1), 3)} secs.")

    # *******************
    # * fixed-incidence *
    # *******************
    print(f"{aktuelleZeit} : calculating fixed-incidence data ...", end="")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)
    LK["IdStaat"] = "00"

    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([0, 1]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["AnzahlFall_7d"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["AnzahlFall"],
        0,
    ).astype(int)
    LK.drop(
        [
            "Meldedatum",
            "NeuerFall",
            "NeuerTodesfall",
            "AnzahlFall",
            "AnzahlTodesfall",
            "NeuGenesen",
            "AnzahlGenesen",
            "Altersgruppe",
            "Geschlecht",
        ],
        inplace=True,
        axis=1,
    )

    agg_key = {"Datenstand": "max", "AnzahlFall_7d": "sum"}
    LK = LK.groupby(
        by=["IdStaat", "IdBundesland", "IdLandkreis"], as_index=False, observed=True
    ).agg(agg_key)

    agg_key = {"IdLandkreis": "max", "Datenstand": "max", "AnzahlFall_7d": "sum"}
    BL = LK.groupby(by=["IdStaat", "IdBundesland"], as_index=False, observed=True).agg(
        agg_key
    )

    agg_key = {
        "IdBundesland": "max",
        "IdLandkreis": "max",
        "Datenstand": "max",
        "AnzahlFall_7d": "sum",
    }
    ID0 = BL.groupby(by=["IdStaat"], as_index=False, observed=True).agg(agg_key)

    LK.drop(["IdStaat", "IdBundesland"], inplace=True, axis=1)
    BL.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    LK_BV_valid = BV[
        (
            (BV["Altersgruppe"] == "A00+")
            & (BV["GueltigAb"] <= Datenstand)
            & (BV["GueltigBis"] >= Datenstand)
            & (BV["AGS"].str.len() == 5)
        )
    ].copy()
    LK_BV_valid.drop(
        ["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    LK_BV_valid.rename(
        columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True
    )
    LK = LK.merge(LK_BV_valid, how="right", on="IdLandkreis")
    LK["AnzahlFall_7d"] = np.where(
        LK["AnzahlFall_7d"].isnull(), 0, LK["AnzahlFall_7d"]
    ).astype(int)
    LK["Datenstand"] = np.where(
        LK["Datenstand"].isnull(), Datenstand.date(), LK["Datenstand"]
    )
    LK["incidence_7d"] = LK["AnzahlFall_7d"] / LK["Einwohner"] * 100000
    LK.drop(["Einwohner"], inplace=True, axis=1)

    BL_BV_valid = BV[
        (
            (BV["Altersgruppe"] == "A00+")
            & (BV["GueltigAb"] <= Datenstand)
            & (BV["GueltigBis"] >= Datenstand)
            & (BV["AGS"].str.len() == 2)
        )
    ].copy()
    BL_BV_valid.drop(
        ["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BL_BV_valid.rename(
        columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True
    )
    BL = BL.merge(BL_BV_valid, how="right", on="IdBundesland")
    BL["AnzahlFall_7d"] = np.where(
        BL["AnzahlFall_7d"].isnull(), 0, BL["AnzahlFall_7d"]
    ).astype(int)
    BL["Datenstand"] = np.where(
        BL["Datenstand"].isnull(), Datenstand.date(), BL["Datenstand"]
    )
    BL["incidence_7d"] = BL["AnzahlFall_7d"] / BL["Einwohner"] * 100000
    BL.drop(["Einwohner"], inplace=True, axis=1)

    LK.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    BL.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    # open existing kum files
    Datenstand2 = Datenstand.date()
    key_list_LK = ["Datenstand", "IdLandkreis"]
    key_list_BL = ["Datenstand", "IdBundesland"]
    key_list_kum = ["D", "I"]
    LK["Datenstand"] = pd.to_datetime(LK["Datenstand"]).dt.date
    BL["Datenstand"] = pd.to_datetime(BL["Datenstand"]).dt.date
    path = os.path.join(repo_path, "dataStore", "frozen-incidence")
    os.makedirs(path, exist_ok=True)

    if Datenstand >= dt.datetime.strptime("2023-04-18", "%Y-%m-%d"):
        if os.path.exists(
            os.path.join(path, "LK.json.xz")
        ) & Datenstand >= dt.datetime.strptime("2023-04-18", "%Y-%m-%d"):
            LK_kum_old = ut.read_json(
                fn=os.path.join(path, "LK.json.xz"), dtype=LK_dtypes
            )
            LK_kum_old["Datenstand"] = pd.to_datetime(LK_kum_old["Datenstand"]).dt.date
            LK_kum_old = LK_kum_old[LK_kum_old["Datenstand"] != Datenstand2]
            LK_kum_old = pd.concat([LK_kum_old, LK])
        else:
            LK_kum_old = LK.copy()
        LK_kum_old.sort_values(by=key_list_LK, inplace=True)
        LK_kum_old["Datenstand"] = LK_kum_old["Datenstand"].astype(str)
        ut.write_json(df=LK_kum_old, fn=os.path.join(path, "LK.json"))

        if os.path.exists(os.path.join(path, "BL.json.xz")):
            BL_kum_old = ut.read_json(
                fn=os.path.join(path, "BL.json.xz"), dtype=BL_dtypes
            )
            BL_kum_old["Datenstand"] = pd.to_datetime(BL_kum_old["Datenstand"]).dt.date
            BL_kum_old = BL_kum_old[BL_kum_old["Datenstand"] != Datenstand2]
            BL_kum_old = pd.concat([BL_kum_old, BL])
        else:
            BL_kum_old = BL.copy()
        BL_kum_old.sort_values(by=key_list_BL, inplace=True)
        BL_kum_old["Datenstand"] = BL_kum_old["Datenstand"].astype(str)
        ut.write_json(df=BL_kum_old, fn=os.path.join(path, "BL.json"))

    LK_kum_new = ut.read_json(
        fn=os.path.join(path, "LK_complete.json.xz"), dtype=kum_dtypes
    )
    BL_kum_new = ut.read_json(
        fn=os.path.join(path, "BL_complete.json.xz"), dtype=kum_dtypes
    )
    LK_kum_new["D"] = pd.to_datetime(LK_kum_new["D"]).dt.date
    BL_kum_new["D"] = pd.to_datetime(BL_kum_new["D"]).dt.date
    LK_kum_new = LK_kum_new[LK_kum_new["D"] != Datenstand2]
    BL_kum_new = BL_kum_new[BL_kum_new["D"] != Datenstand2]

    LK.rename(
        columns={
            "Datenstand": "D",
            "IdLandkreis": "I",
            "Landkreis": "T",
            "incidence_7d": "i",
        },
        inplace=True,
    )
    BL.rename(
        columns={
            "Datenstand": "D",
            "IdBundesland": "I",
            "Bundesland": "T",
            "incidence_7d": "i",
        },
        inplace=True,
    )

    LK_kum_new = pd.concat([LK_kum_new, LK])
    LK_kum_new.sort_values(by=key_list_kum, inplace=True)
    BL_kum_new = pd.concat([BL_kum_new, BL])
    BL_kum_new.sort_values(by=key_list_kum, inplace=True)

    LK_kum_new["D"] = LK_kum_new["D"].astype(str)
    BL_kum_new["D"] = BL_kum_new["D"].astype(str)

    ut.write_json(df=LK_kum_new, fn=os.path.join(path, "LK_complete.json"))
    ut.write_json(df=BL_kum_new, fn=os.path.join(path, "BL_complete.json"))

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round(t2 - t1, 3)} secs.")
    print(f"{aktuelleZeit} : Fallzahlen update ...", end="")
    t1 = time.time()
    fallzahlen_update.fallzahlen_update(feather_path)
    t2 = time.time()
    endTime = dt.datetime.now()
    os.remove(feather_path)
    aktuelleZeit = endTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f" done in {round(t2 - t1, 3)} secs.")
    print(f"{aktuelleZeit} : total python time: {endTime - startTime} .")
