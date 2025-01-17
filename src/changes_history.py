import os
import datetime as dt
import pandas as pd
import utils as ut
from time import time


def update(LK: pd.DataFrame, BL: pd.DataFrame, Datenstand, repo_path):
    HC_dtp = {"i": "str", "m": "object", "c": "int64"}
    HD_dtp = {"i": "str", "m": "object", "d": "int64"}
    HR_dtp = {"i": "str", "m": "object", "r": "int64"}

    HCC_dtp = {"i": "str", "m": "object", "c": "int64", "dc": "int64", "cD": "object"}
    HCD_dtp = {"i": "str", "m": "object", "d": "int64", "cD": "object"}
    HCR_dtp = {"i": "str", "m": "object", "r": "int64", "cD": "object"}

    LK.rename(
        columns={
            "IdLandkreis": "i",
            "Landkreis": "t",
            "Meldedatum": "m",
            "cases": "c",
            "deaths": "d",
            "recovered": "r",
            "cases7d": "c7",
            "incidence7d": "i7",
        },
        inplace=True,
    )
    BL.rename(
        columns={
            "IdBundesland": "i",
            "Bundesland": "t",
            "Meldedatum": "m",
            "cases": "c",
            "deaths": "d",
            "recovered": "r",
            "cases7d": "c7",
            "incidence7d": "i7",
        },
        inplace=True,
    )

    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    # split LK
    Lc = LK[["i", "m", "c"]].copy()
    Ld = LK[["i", "m", "d"]].copy()
    Lr = LK[["i", "m", "r"]].copy()

    # split BL
    Bc = BL[["i", "m", "c"]].copy()
    Bd = BL[["i", "m", "d"]].copy()
    Br = BL[["i", "m", "r"]].copy()

    # read compressed, but store uncompressed! compressing will be done in bash script with 7zip
    LKcasesJsonFull = os.path.join(repo_path, "dataStore", "history", "d_cases_short.json.xz")
    LKdeathsJsonFull = os.path.join(repo_path, "dataStore", "history", "d_deaths_short.json.xz")
    LKrecoveredJsonFull = os.path.join(repo_path, "dataStore", "history", "d_recovered_short.json.xz")

    BLcasesJsonFull = os.path.join(repo_path, "dataStore", "history", "s_cases_short.json.xz")
    BLdeathsJsonFull = os.path.join(repo_path, "dataStore", "history", "s_deaths_short.json.xz")
    BLrecoveredJsonFull = os.path.join(repo_path, "dataStore", "history", "s_recovered_short.json.xz")

    readJson = 0
    writeJson = 0

    # calculate diff data
    if os.path.exists(LKcasesJsonFull):
        t1 = time()
        oLc = ut.read_json(fn=LKcasesJsonFull, dtype=HC_dtp)
        readJson += time() - t1
        LDc = ut.get_different_rows(oLc, Lc)
        LDc.set_index(["i", "m"], inplace=True, drop=False)
        oLc.set_index(["i", "m"], inplace=True, drop=False)
        LDc["dc"] = LDc["c"] - oLc["c"]
        LDc["dc"] = LDc["dc"].fillna(LDc["c"]).astype(int)
        LDc.reset_index(inplace=True, drop=True)
        oLc.reset_index(inplace=True, drop=True)
    else:
        LDc = Lc.copy()
        LDc["dc"] = LDc["c"]
    t1 = time()
    ut.write_json(df=Lc, fn=LKcasesJsonFull[:-3])
    writeJson += time() - t1
    
    if os.path.exists(LKdeathsJsonFull):
        t1 = time()
        oLd = ut.read_json(fn=LKdeathsJsonFull, dtype=HD_dtp)
        readJson += time() - t1
        LDd = ut.get_different_rows(oLd, Ld)
    else:
        LDd = Ld.copy()
    t1 = time()
    ut.write_json(df=Ld, fn=LKdeathsJsonFull[:-3])
    writeJson += time() - t1

    if os.path.exists(LKrecoveredJsonFull):
        t1 = time()
        oLr = ut.read_json(fn=LKrecoveredJsonFull, dtype=HR_dtp)
        readJson += time() - t1
        LDr = ut.get_different_rows(oLr, Lr)
    else:
        LDr = Lr.copy()
    t1 = time()
    ut.write_json(df=Lr, fn=LKrecoveredJsonFull[:-3])
    writeJson += time() - t1

    if os.path.exists(BLcasesJsonFull):
        t1 = time()
        oBc = ut.read_json(fn=BLcasesJsonFull, dtype=HC_dtp)
        readJson += time() - t1
        BDc = ut.get_different_rows(oBc, Bc)
        BDc.set_index(["i", "m"], inplace=True, drop=False)
        oBc.set_index(["i", "m"], inplace=True, drop=False)
        BDc["dc"] = BDc["c"] - oBc["c"]
        BDc["dc"] = BDc["dc"].fillna(BDc["c"]).astype(int)
        BDc.reset_index(inplace=True, drop=True)
        oBc.reset_index(inplace=True, drop=True)
    else:
        BDc = Bc.copy()
        BDc["dc"] = BDc["c"]
    t1 = time()
    ut.write_json(df=Bc, fn=BLcasesJsonFull[:-3])
    writeJson += time() - t1

    if os.path.exists(BLdeathsJsonFull):
        t1 = time()
        oBd = ut.read_json(fn=BLdeathsJsonFull, dtype=HD_dtp)
        readJson += time() - t1
        BDd = ut.get_different_rows(oBd, Bd)
    else:
        BDd = Bd.copy()
    t1 = time()
    ut.write_json(df=Bd, fn=BLdeathsJsonFull[:-3])
    writeJson += time() - t1

    if os.path.exists(BLrecoveredJsonFull):
        t1 = time()
        oBr = ut.read_json(fn=BLrecoveredJsonFull, dtype=HR_dtp)
        readJson += time() - t1
        BDr = ut.get_different_rows(oBr, Br)
    else:
        BDr = Br.copy()
    t1 = time()
    ut.write_json(df=Br, fn=BLrecoveredJsonFull[:-3])
    writeJson += time() - t1

    ChangeDate = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LDc["cD"] = ChangeDate
    LDd["cD"] = ChangeDate
    LDr["cD"] = ChangeDate

    BDc["cD"] = ChangeDate
    BDd["cD"] = ChangeDate
    BDr["cD"] = ChangeDate

    # create pathes if not exists
    os.makedirs(os.path.join(repo_path, "dataStore", "historychanges", "cases"), exist_ok=True)
    os.makedirs(os.path.join(repo_path, "dataStore", "historychanges", "deaths"), exist_ok=True)
    os.makedirs(os.path.join(repo_path, "dataStore", "historychanges", "recovered"), exist_ok=True)

    LKDiffCasesJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "cases", "districts_Diff.json.xz")
    LKDiffDeathsJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "deaths", "districts_Diff.json.xz")
    LKDiffRecoveredJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "recovered", "districts_Diff.json.xz")

    BLDiffCasesJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "cases", "states_Diff.json.xz")
    BLDiffDeathsJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "deaths", "states_Diff.json.xz")
    BLDiffRecoveredJsonFull = os.path.join(repo_path, "dataStore", "historychanges", "recovered", "states_Diff.json.xz")
    readolddiff = 0
    writenewdiff = 0

    if os.path.exists(LKDiffCasesJsonFull):
        t1 = time() 
        oLDc = ut.read_json(fn=LKDiffCasesJsonFull, dtype=HCC_dtp)
        readolddiff += time() - t1
        LDc = pd.concat([oLDc, LDc])
    LDc.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=LDc, fn=LKDiffCasesJsonFull[:-3])
    writenewdiff += time() - t1

    if os.path.exists(LKDiffDeathsJsonFull):
        t1 = time()
        oLDd = ut.read_json(fn=LKDiffDeathsJsonFull, dtype=HCD_dtp)
        readolddiff += time() - t1
        LDd = pd.concat([oLDd, LDd])
    LDd.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=LDd, fn=LKDiffDeathsJsonFull[:-3])
    writenewdiff += time() - t1

    if os.path.exists(LKDiffRecoveredJsonFull):
        t1 = time()
        oLDr = ut.read_json(fn=LKDiffRecoveredJsonFull, dtype=HCR_dtp)
        readolddiff += time() - t1
        LDr = pd.concat([oLDr, LDr])
    LDr.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=LDr, fn=LKDiffRecoveredJsonFull[:-3])
    writenewdiff += time() - t1

    if os.path.exists(BLDiffCasesJsonFull):
        t1 = time()
        oBDc = ut.read_json(fn=BLDiffCasesJsonFull, dtype=HCC_dtp)
        readolddiff += time() - t1
        BDc = pd.concat([oBDc, BDc])
    BDc.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=BDc, fn=BLDiffCasesJsonFull[:-3])
    writenewdiff += time() - t1

    if os.path.exists(BLDiffDeathsJsonFull):
        t1 = time()
        oBDd = ut.read_json(fn=BLDiffDeathsJsonFull, dtype=HCD_dtp)
        readolddiff += time() - t1
        BDd = pd.concat([oBDd, BDd])
    BDd.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=BDd, fn=BLDiffDeathsJsonFull[:-3])
    writenewdiff += time() - t1

    if os.path.exists(BLDiffRecoveredJsonFull):
        t1 = time()
        oBDr = ut.read_json(fn=BLDiffRecoveredJsonFull, dtype=HCR_dtp)
        readolddiff += time() - t1
        BDr = pd.concat([oBDr, BDr])
    BDr.sort_values(by=["i", "m", "cD"], inplace=True)
    t1 = time()
    ut.write_json(df=BDr, fn=BLDiffRecoveredJsonFull[:-3])
    writenewdiff += time() - t1

    return [readJson, writeJson, readolddiff, writenewdiff]
