import re
import os
import json

from tqdm import tqdm
from pathlib import Path

import multiprocessing
from functools import partial
from concurrent.futures import ThreadPoolExecutor


from utils import get_mongo_collection


processed_data_coll = get_mongo_collection("libgen_data_2", connect=False)
table_coll = get_mongo_collection("table_collection", connect=False)


def create_markdown_table(rows: list, columns: list = None):
    # If columns are provided, create the header row
    if columns:
        header_row = "| " + " | ".join(columns) + " |"
        separator_row = "| " + " | ".join(["---"] * len(columns)) + " |"
    else:
        header_row = ""
        separator_row = ""

    # Create the data rows
    data_rows = ["| " + " | ".join(map(str, row)) + " |" for row in rows]

    # Combine all parts into the final markdown table string
    table = "\n".join([header_row, separator_row] + data_rows if columns else data_rows)

    return table


def process_page(page_data: dict):
    matches = re.findall(r"(?<={{)\w+:.*?(?=}})", page_data["text"])

    for match in matches:
        _type, _id = match.split(":")
        if _type in ["equation", "figure"]:
            caption = ""
            for elem in page_data[f"{_type}s"]:
                if elem["id"] == _id:
                    caption = (
                        elem["caption"]
                        if _type == "figure"
                        else elem.get("text_to_speech", "") or elem.get("text", "")
                    )
                    caption = caption.strip()
                    break

            page_data["text"] = page_data["text"].replace("{{" + match + "}}", caption)
        else:
            table = table_coll.find_one({"tableId": _id})
            table_md = ""
            if table and len(table["table_data"].get("data", {}).get("rows", [])):
                rows = table["table_data"]["data"]["rows"]
                table_md = create_markdown_table(
                    rows=rows[1:] if len(rows) > 1 else rows,
                    columns=rows[0] if len(rows) > 1 else None,
                )

            page_data["text"] = page_data["text"].replace(
                "{{" + match + "}}", f"\n{table_md}\n"
            )

    return page_data["text"]


def process_pdfs(pdf: dict, savedir: str):
    try:
        results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(process_page, page) for page in pdf["pages"]]
            for future in futures:
                results.append(future.result())

        agg_text = "".join(results)
        if not agg_text:
            raise ValueError

        with open(
            os.path.join(savedir, f"{os.path.splitext(pdf['book'])[0]}.json"),
            "w",
        ) as file:
            json.dump({"text": agg_text}, file)
    except Exception as e:
        return {
            "_id": str(pdf["_id"]),
            "book_id": pdf["bookId"],
            "filename": pdf["book"],
            "error": str(e),
        }


if __name__ == "__main__":
    savedir = os.path.join("LTIM_DATA", "DATASET")
    pdf_savedir = os.path.join(savedir, "PDFs")
    Path(pdf_savedir).mkdir(exist_ok=True, parents=True)

    pipeline = [
        {"$sort": {"_id": -1}},
        {"$group": {"_id": "$bookId", "document": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$document"}},
    ]
    pdfs = list(processed_data_coll.aggregate(pipeline))

    _process_pdfs = partial(process_pdfs, savedir=pdf_savedir)
    failed_pdfs = []

    with multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1) as pool:
        with tqdm(total=len(pdfs)) as pbar:
            for result in pool.imap_unordered(_process_pdfs, pdfs):
                if isinstance(result, dict):
                    failed_pdfs.append(result)

                pbar.update(1)

    pbar.close()

    if len(failed_pdfs):
        with open(os.path.join(savedir, "__failed.jsonl"), "w") as file:
            for lno, line in enumerate(failed_pdfs):
                string = json.dumps(line)
                if lno == len(failed_pdfs) - 1:
                    string += "\n"

                file.write(string)

    dataset_sample_paths = list(Path(pdf_savedir).glob("**/*.json"))
    dataset_fp = open(os.path.join(savedir, "dataset.jsonl"), "w")

    for num, filepath in enumerate(tqdm(dataset_sample_paths)):
        with open(filepath, "r") as file:
            string = file.read().strip()

        if num < len(dataset_sample_paths) - 1:
            string += "\n"

        dataset_fp.write(string)

    dataset_fp.close()
