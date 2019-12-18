import os
import json
from tqdm import tqdm

chars = {}
for root, dirs, files in os.walk("."):
    for fp in tqdm(files):
        if ".txt" in fp:
            with open(os.path.join(root, fp)) as in_file:
                for line in in_file:
                    for char in line:
                        if char.upper() != char:
                            print(os.path.join(root, fp))
                        try:
                            chars[char] += 1
                        except KeyError:
                            chars[char] = 1

chars = [char for char in chars.keys() if char != "\n"]
with open("guarani_labels.json", "w") as out_file:
    json.dump(chars, out_file, ensure_ascii=False)
