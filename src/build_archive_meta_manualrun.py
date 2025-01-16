import os
import json
import sys
import datetime as dt

try:
    base_path = os.path.dirname(os.path.abspath(__file__))
    meta_path = os.path.normpath(
        os.path.join(
            base_path, "..", "dataStore", "meta"
        )
    )
    datafilePath = os.path.normpath(
        os.path.join(
            base_path, "..", "..", "RKIData"
        )
    )
    filename_meta_old = "meta.json"
    filename_meta_new = "meta_new.json"
    date = dt.datetime.fromisoformat(sys.argv[1])
    date_only_str = date.strftime("%Y-%m-%d")

    filename = "RKI_COVID19_" + date_only_str + ".csv.xz"
    url_data = os.path.normpath(
        os.path.join(
            datafilePath, filename
        )
    )

    size = os.path.getsize(url_data)

    with open(meta_path + "/" + filename_meta_old, "r", encoding="utf8") as file:
        metaObj = json.load(file)

    metaObj["publication_date"] = sys.argv[1]
    metaObj["version"] = date_only_str
    metaObj["size"] = size
    metaObj["filename"] = filename
    metaObj["url"] = url_data
    modified = int(dt.datetime.timestamp(date)) * 1000
    metaObj["modified"] = modified

    with open(meta_path + "/" + filename_meta_new, "w", encoding="utf8") as json_file:
        json.dump(metaObj, json_file, ensure_ascii=False)

except Exception as e:
    print(e)
