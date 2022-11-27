import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import time

BASE_URL = "https://www.ccsd.org.uk/ccsdschedule/CCSDScheduleCode?ctype=3&inactives=1&searchvalue="

TABLE_URL_BASE = "https://www.ccsd.org.uk/ccsdschedule/CCSDScheduleCode?rt=&st=&Chapter=&Section=&Subsection=&page="

TABLE_URL_EXTENSION = "&ctype=3&numitems=20&searchvalue=&inactives=1"


def get_max_pages(url: str) -> int:
    """
    Get the maximum number of pages to scrape. This will be used in the formula to
    scrape the whole table

    Args:
        url (str): The url of the website that is to be scrappeed

    Returns:
        int: The number of pages in the website
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    max_number_of_page_to_scrape = soup.find("div", class_="pagination").text[-3:]
    return int(max_number_of_page_to_scrape)


def get_codes(table: str) -> list:
    """
    Retrieve the CCSD codes from the CCSD Schedule table

    Args:
        table (str): The NavigableString which the table is converted into

    Returns:
        list: The codes from the first column of the table
    """
    code_list = []
    for codes in table:
        code_class = ["greenButt butt1", "greenButt butt0"]
        code = codes.find("a", class_=code_class)
        if code is not None:
            code = code.text
            code_list.append(code)
    return code_list


def get_description(table: str) -> list:
    """
    Retrieve the description of each CCSD code

    Args:
        table (str): The NavigableString which the table is converted into

    Returns:
        list: The description of each code from the second column of the table
    """
    description_list = []
    for descriptions in table:
        description = descriptions.find("a", class_="codelink")
        if description is not None:
            description = description.text
            description_list.append(description)
    return description_list


def get_chapter(table: str) -> list:
    """
    Retrieve the chapter number for each CCSD code

    Args:
        table (str): The NavigableString which the table is converted into

    Returns:
        list: The chapter number of each code from the third column of the table
    """
    chapter_list = []
    for chapters in table:
        try:
            chapter = chapters.find_all("a", class_="codelink")[1].text
        except IndexError:
            pass
        chapter_list.append(chapter)
    return chapter_list


def get_guidance(table: str) -> list:
    """
    Retrieve the guidance/miscellaneous information for each CCSD code

    Args:
        table (str): The NavigableString which the table is converted into

    Returns:
        list: Any misc. information regarding a code (for eg: "This code replaces XXXX") is extracted from the third column of the table
    """
    guidance_list = []
    for guidances in table:
        try:
            guidance = guidances.find_all("td")[3].text.strip()
        except IndexError:
            pass
        guidance_list.append(guidance)
    return guidance_list

# The code below will scrape the CCSD schedule table and save it as a .csv file.
# The unacceptable combinations will be scraped separately

last_page_number = get_max_pages(BASE_URL)

ccsd_schedule = {"code": [], "description": [], "chapter": [], "guidance": []}

for page_number in range(0, last_page_number):
    url = f"{TABLE_URL_BASE}{str(page_number)}{TABLE_URL_EXTENSION}"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    medical_codes_table = soup.find("table").find_all("tr")[1:]

    current_page_data = {
        "code": get_codes(medical_codes_table),
        "description": get_description(medical_codes_table),
        "chapter": get_chapter(medical_codes_table),
        "guidance": get_guidance(medical_codes_table),
    }
    for key in ccsd_schedule:
        ccsd_schedule[key].extend(current_page_data[key])
time.sleep(0.5)
ccsd_df = pd.DataFrame.from_records(ccsd_schedule)

ccsd_df.to_csv("ccsd.csv")


# This part of the code will scrape the unacceptable combinations of diagnosis with any procedure

code_class = ["greenButt butt1", "greenButt butt0"]
ccsd_schedule = []
for page_number in range(0, last_page_number):
    url = f"{TABLE_URL_BASE}{str(page_number)}{TABLE_URL_EXTENSION}"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    codes_table = soup.find("table").find_all("tr")[1:]

    for codes in codes_table:
        code = codes.find("a", class_="greenButt butt1") or codes.find(
            "a", class_="greenButt butt0"
        )
        if code is not None:
            code = code.text
        try:
            unacceptable_combination_link = codes.find(
                "a", class_="codelink"
            ).attrs["href"]
        except AttributeError:
            pass
        except KeyError:
            pass

        unacceptable_combination_page = requests.get(
            f"https://www.ccsd.org.uk{unacceptable_combination_link}",
            headers=headers,
        )
        soup2 = BeautifulSoup(unacceptable_combination_page.text, "html.parser")
        codes_table2 = soup2.find("table", class_="codeTr").find_all(
            "a", class_=code_class
        )

        bad_combo = []
        for data in codes_table2:
            bad_combo.append(data.text)

        codes_info = {"code": code, "unacceptable combinations": bad_combo}
        ccsd_schedule.append(codes_info)

    print("Codes Found:", len(ccsd_schedule))
    time.sleep(0.3)

bad_combo_df = pd.DataFrame(ccsd_schedule)
expanded_bad_combo_df = bad_combo_df.explode("unacceptable combinations")

expanded_bad_combo_df.to_csv("unacceptable_combinations.csv")