import os
from PyPDF2 import PdfReader
from tqdm import tqdm
import pandas as pd
from enum import Enum
import re

WELLS_FARGO_CREDIT_CARD_REGEX: str = r"[0-9]{2}\/[0-9]{2}.*[0-9]+\.[0-9]{2}"
WELLS_FARGO_CHECKING_REGEX: str = r"[0-9]{1,2}\/[0-9]{1,2}.*(\n.*[0-9,]+\.[0-9]{2})?"
YEAR: int = 2023


class FileType(Enum):
    PDF: str = ".pdf"
    CSV: str = ".csv"


# pylint: disable-next:
class Accounts(Enum):
    CHASE_CREDIT_CARD: str = "CCC"
    WELLS_FARGO_CHECKING: str = "WFCh"
    WELLS_FARGO_CREDIT_CARD: str = "WFCC"
    FIRST_REPUBLIC_CHECKING: str = "FRC"
    FIRST_REPUBLIC_MORTGAGE: str = "FRM"


def find_account(filename_path: str) -> Accounts:
    for account in Accounts:
        if account.value in filename_path:
            return account
    raise RuntimeError(f"No account found for {filename_path}")


def wf_cc_parse_pdf(
    account_origin: Accounts, pdf_path: str, regex: str
) -> pd.DataFrame:
    with open(pdf_path, "rb") as pdf_file_obj:
        pdf_reader = PdfReader(pdf_file_obj)
        text = ""
        for page in tqdm(pdf_reader.pages, total=len(pdf_reader.pages)):
            text += page.extract_text()
    lines = re.findall(regex, text)
    dates = [
        re.search(r"^[0-9]{1,2}/[0-9]{1,2}", l).group(0) + f"/{YEAR}" for l in lines
    ]
    money = [re.search(r"[0-9?,]*[0-9]+\.[0-9]{2}$", l).group(0) for l in lines]
    description = []
    for l, d in zip(lines, dates):
        match = re.search(r"[0-9]{2}\/[0-9]{2}.{24}(.*)", l)
        if match is None:
            description.append("")
        else:
            description.append(match.group(1).replace(d, ""))
    df = pd.DataFrame(
        data={
            "Date": dates,
            "Description": description,
            "Amount": money,
            "Account": [account_origin.name] * len(lines),
        }
    )
    return df


def wf_ch_parse_pdf(
    account_origin: Accounts, pdf_path: str, regex: str
) -> pd.DataFrame:
    with open(pdf_path, "rb") as pdf_file_obj:
        pdf_reader = PdfReader(pdf_file_obj)
        text = ""
        for page in tqdm(pdf_reader.pages, total=len(pdf_reader.pages)):
            text += page.extract_text()
    lines = re.findall(regex, text)
    import IPython

    IPython.embed()
    # .group(0)[:-1] + "/2023"
    dates = [re.search(r"^[0-9]{1,2}/[0-9]{1,2}", l).group(0) + "/2023" for l in lines]
    money = [re.search(r"[0-9?,]*[0-9]+\.[0-9]{2}$", l).group(0) for l in lines]
    description = []
    for l, d in zip(lines, dates):
        match = re.search(r"[0-9]{2}\/[0-9]{2}.{24}(.*)", l)
        if match is None:
            description.append("")
        else:
            description.append(match.group(1).replace(d, ""))
    df = pd.DataFrame(
        data={
            "Date": dates,
            "Description": description,
            "Amount": money,
            "Account": [account_origin.name] * len(lines),
        }
    )
    return df


def main(args: "Namespace") -> None:
    # collect all files
    all_statements_as_dataframes = []
    for dirpath, _dirnames, filenames in tqdm(os.walk("./statements/")):
        for some_file in filenames:
            path = os.path.join(dirpath, some_file)
            account_origin = find_account(path)
            print(f"Account is {account_origin}")
            if FileType.PDF.value in some_file:
                if account_origin == Accounts.WELLS_FARGO_CHECKING:
                    pdf_as_df = wf_ch_parse_pdf(
                        account_origin,
                        path,
                        WELLS_FARGO_CHECKING_REGEX,
                    )
                    all_statements_as_dataframes.append(pdf_as_df)
                if account_origin == Accounts.WELLS_FARGO_CREDIT_CARD:
                    pdf_as_df = wf_cc_parse_pdf(
                        account_origin,
                        path,
                        WELLS_FARGO_CREDIT_CARD_REGEX,
                    )
                    all_statements_as_dataframes.append(pdf_as_df)

            if FileType.CSV.value in some_file:
                if account_origin == Accounts.CHASE_CREDIT_CARD:
                    csv_df = pd.read_csv(path)
                    csv_df = csv_df.loc[:, ["Post Date", "Description", "Amount"]]
                    csv_df["Account"] = [account_origin.name] * len(csv_df)
                    csv_df["Amount"] = -1 * csv_df["Amount"]
                    csv_df.rename(columns={"Post Date": "Date"}, inplace=True)
                    all_statements_as_dataframes.append(csv_df)
                if account_origin == Accounts.FIRST_REPUBLIC_CHECKING:
                    csv_df = pd.read_csv(path)
                    csv_df["Amount"] = -1 * (csv_df["Debit"] + csv_df["Credit"])
                    csv_df = csv_df.loc[:, ["Date", "Statement Description", "Amount"]]
                    csv_df["Account"] = [account_origin.name] * len(csv_df)
                if account_origin == Accounts.FIRST_REPUBLIC_MORTGAGE:
                    csv_df = pd.read_csv(path)
                    csv_df["Amount"] = -1 * (
                        csv_df["Debit"].fillna(0) + csv_df["Credit"]
                    )
                    csv_df = csv_df.loc[:, ["Date", "Statement Description", "Amount"]]
                    csv_df["Account"] = [account_origin.name] * len(csv_df)
    # print("\n".join(all_statements_as_text)[5000:7000])
    df = pd.concat(all_statements_as_dataframes)
    df.to_csv("./aggregated_statement.csv")


if __name__ == "__main__":
    main(None)
