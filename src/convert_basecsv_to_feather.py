import os
import utils as ut
import re
import time
import pandas as pd
# converts all csv data files from csv to feather format for faster loading

dtypes = {
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
base_path = os.path.dirname(os.path.abspath(__file__))
path = os.path.normpath(os.path.join(base_path, "..", "..", "RKIData"))

# limit RKI_COVID19 Data files to the last 30 days
iso_date_re = "([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])"
file_list = os.listdir(path)
file_list.sort(reverse=False)
pattern = "RKI_COVID19"
pattern2 ="^.*\.(feather)$"
all_files = []
for file in file_list:
  file_path_full = os.path.join(path, file)
  if not os.path.isdir(file_path_full):
    filename = os.path.basename(file)
    re_filename = re.search(pattern, filename)
    re_search = re.search(iso_date_re, filename)
    re_feather = re.search(pattern2, filename)
    if re_search and re_filename and not re_feather:
      all_files.append((file_path_full))

for path in all_files:
  t1 = time.time()
  LK = pd.read_csv(path, engine="pyarrow", usecols=dtypes.keys(), dtype=dtypes)
  LK = ut.squeeze_dataframe(LK)
  featherPath = path.replace("csv", "feather")[:-3]
  ut.write_file(df=LK, fn=featherPath, compression="lz4")
  os.remove(path)
  t2 = time.time()
  print(f"{path}; {LK.shape[0]} rows. {round(t2 - t1, 3)} secs.")

